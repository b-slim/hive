/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Clock eviction policy. Uses a simple circular list to keep a ring of current used buffers.
 */
public class ClockCachePolicy implements LowLevelCachePolicy {

  static final int DEFAULT_MAX_CIRCLES = 5;
  /**
   * The clock hand shared between threads thus made volatile.
   * Will be null at start, On each insert we add the new item as new tail.
   */
  private volatile LlapCacheableBuffer clockHand;
  /**
   * Global lock used to exclusive access to the linked ring including head modification.
   */
  private final Lock lock = new ReentrantLock();

  private EvictionListener evictionListener;
  /**
   * Max number of clock rotation before giving up on clock operation like eviction.
   */
  private final int maxCircles;

  public ClockCachePolicy() {
    maxCircles = DEFAULT_MAX_CIRCLES;
  }

  public ClockCachePolicy(int maxCircles) {
    Preconditions.checkState(maxCircles > 0, "Maximum number of clock rotation must be positive and got " + maxCircles);
    this.maxCircles = maxCircles;
  }

  /**
   * Signals to the policy the addition of a new entry to the cache. An entry come with a priority that can be used as
   * a hint to replacement policy.
   *
   * @param buffer   buffer to be cached
   * @param priority the priority of cached element
   */
  @Override public void cache(LlapCacheableBuffer buffer, LowLevelCache.Priority priority) {
    lock.lock();
    try {
      clockHand = appendToCircularList(clockHand, buffer);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Appends new entry to the tail of circular list.
   *
   * @param head   circular list head.
   * @param buffer new entry to be added.
   * @return the ring head.
   */
  private static LlapCacheableBuffer appendToCircularList(LlapCacheableBuffer head, LlapCacheableBuffer buffer) {
    if (head == null) {
      return linkToItSelf(buffer);
    }
    buffer.next = head;
    buffer.prev = head.prev;
    head.prev.next = buffer;
    head.prev = buffer;
    return head;
  }

  /**
   * Links the entry to it self to form a ring.
   *
   * @param buffer input
   * @return buffer
   */
  private static LlapCacheableBuffer linkToItSelf(LlapCacheableBuffer buffer) {
    buffer.prev = buffer;
    buffer.next = buffer;
    return buffer;
  }

  @Override public void notifyLock(LlapCacheableBuffer buffer) {
    buffer.setClockBit();
  }

  /**
   * Notifies the policy that a buffer is unlocked after been used. This notification signals to the policy that an
   * access to this page occurred.
   *
   * @param buffer buffer that just got unlocked after a read.
   */
  @Override public void notifyUnlock(LlapCacheableBuffer buffer) {

  }

  /**
   * Signals to the policy that it has to evict some entries from the cache.
   * Policy has to at least evict the amount memory requested.
   * Not that is method will block until at least {@code memoryToReserve} bytes are evicted.
   *
   * @param memoryToReserve amount of bytes to be evicted
   * @return actual amount of evicted bytes.
   */
  @Override public long evictSomeBlocks(long memoryToReserve) {
    long evicted = 0;
    if (clockHand == null) {
      return evicted;
    }
    int fullClockRotation = 0;
    lock.lock();
    try {
      LlapCacheableBuffer lastBuffer = clockHand.prev;
      while (evicted < memoryToReserve && clockHand != null && fullClockRotation < maxCircles) {
        if (lastBuffer == clockHand) {
          fullClockRotation++;
        }
        if (clockHand.isClockBitSet()) {
          //mark it as ready to be removed
          clockHand.unSetClockBit();
          clockHand = clockHand.next;
        } else {
          // try to evict this victim
          if (clockHand.invalidate() == LlapCacheableBuffer.INVALIDATE_OK) {
            evictionListener.notifyEvicted(clockHand);
            evicted += clockHand.getMemoryUsage();
            LlapCacheableBuffer newHand = clockHand.next;
            if (newHand == clockHand) {
              clockHand = null;
            } else {
              //remove it from the ring.
              if (clockHand == lastBuffer) {
                lastBuffer = clockHand.prev;
              }
              clockHand.prev.next = newHand;
              newHand.prev = clockHand.prev;
              clockHand = newHand;
            }
          } else {
            // can not be evicted set it back at candidate to next cycle
            clockHand.unSetClockBit();
            clockHand = clockHand.next;
          }
        }
      }
      return evicted;
    } finally {
      lock.unlock();
    }
  }

  @Override public void setEvictionListener(EvictionListener listener) {
    evictionListener = listener;
  }

  @Override public long purge() {
    return evictSomeBlocks(Long.MAX_VALUE);
  }

  @Override public void debugDumpShort(StringBuilder sb) {
    if (clockHand == null) {
      sb.append("Clock is empty");
      return;
    }
    lock.lock();
    try {
      sb.append("Clock Status\n");
      LlapCacheableBuffer currentClockHand = clockHand;
      LlapCacheableBuffer lastElement = clockHand.prev;
      while (currentClockHand != lastElement) {
        sb.append(currentClockHand.toStringForCache());
        currentClockHand = currentClockHand.next;
      }
      sb.append(lastElement.toStringForCache());
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting public LlapCacheableBuffer getClockHand() {
    return clockHand;
  }
}
