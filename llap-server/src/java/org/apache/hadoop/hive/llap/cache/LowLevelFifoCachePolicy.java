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

import java.util.Queue;

import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

public class LowLevelFifoCachePolicy implements LowLevelCachePolicy {

  private final Queue<LlapCacheableBuffer> buffers = new ConcurrentArrayQueue<>();
  private EvictionListener evictionListener;

  public LowLevelFifoCachePolicy() {
    LlapIoImpl.LOG.info("FIFO cache policy");
  }

  @Override public void cache(LlapCacheableBuffer buffer, Priority pri) {
    buffers.add(buffer);
  }

  @Override public void notifyLock(LlapCacheableBuffer buffer) {
    // FIFO policy doesn't care.
  }

  @Override public void notifyUnlock(LlapCacheableBuffer buffer) {
    // FIFO policy doesn't care.
  }

  @Override public void setEvictionListener(EvictionListener listener) {
    this.evictionListener = listener;
  }

  @Override public long purge() {
    long evicted = evictSomeBlocks(Long.MAX_VALUE);
    LlapIoImpl.LOG.info("PURGE: evicted {} from FIFO policy", LlapUtil.humanReadableByteCount(evicted));
    return evicted;
  }

  @Override public long evictSomeBlocks(long memoryToReserve) {
    return evictInternal(memoryToReserve, -1);
  }

  private long evictInternal(long memoryToReserve, int minSize) {
    long evicted = 0;
    int attempts = 0;
    while (evicted < memoryToReserve && !buffers.isEmpty() && attempts < 10) {
      LlapCacheableBuffer buffer = buffers.poll();
      if (buffer == null) {
        attempts++;
        continue;
      }
      long memUsage = buffer.getMemoryUsage();
      if (memUsage < minSize || (minSize > 0 && !(buffer instanceof LlapAllocatorBuffer))) {
        continue;
      }
      int result = buffer.invalidate();
      if (LlapCacheableBuffer.INVALIDATE_OK == result) {
        evicted += memUsage;
        evictionListener.notifyEvicted(buffer);
      } else if (result == LlapCacheableBuffer.INVALIDATE_FAILED) {
        buffers.offer(buffer);
      }
    }
    return evicted;
  }
  @Override public void debugDumpShort(StringBuilder sb) {
    sb.append("\nFIFO eviction list: ");
    sb.append(buffers.size()).append(" elements)");
  }
}
