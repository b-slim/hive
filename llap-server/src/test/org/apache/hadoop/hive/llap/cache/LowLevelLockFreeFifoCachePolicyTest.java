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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.llap.cache.ClockCachePolicyTest.getLlapCacheableBuffers;
import static org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer.INVALIDATE_OK;

/**
 * Test for fifo policy.
 */
@RunWith(Parameterized.class) public class LowLevelLockFreeFifoCachePolicyTest {

  @Parameterized.Parameters public static Collection collectioNumbers() {
    return Arrays.asList(new Object[][]{
        { 0 }, { 1 }, { 2 }, { 5 }, { 10009 }, { 20002 } });
  }

  private static final long SEED = 90990;
  private static final int BUFFER_SIZE = 16;
  public static final Predicate<Integer> IS_EVEN = x -> x % 2 == 0;
  EvictionTracker defaultEvictionListener = new EvictionTracker();
  LowLevelLockFreeFifoCachePolicy policy;
  private final int numPages;

  public LowLevelLockFreeFifoCachePolicyTest(int numPages) {
    this.numPages = numPages;
  }

  @Test public void testEvictionOneByOne() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages);
    Collections.shuffle(access, new Random(SEED));
    policy = new LowLevelLockFreeFifoCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    while (policy.peek() != null) {
      // should not over evict
      Assert.assertEquals(BUFFER_SIZE, policy.evictSomeBlocks(BUFFER_SIZE));
    }
    Assert.assertEquals(defaultEvictionListener.evicted.size(), numPages);
    for (int i = 0; i < access.size(); i++) {
      Assert.assertEquals(defaultEvictionListener.evicted.get(i), access.get(i));
    }
  }

  @Test public void testEvictionWithLockedBuffers() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages, IS_EVEN);
    Collections.shuffle(access, new Random(SEED));
    policy = new LowLevelLockFreeFifoCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    List<LlapCacheableBuffer>
        unlockedBuffers =
        access.stream()
            .filter(llapCacheableBuffer -> llapCacheableBuffer.invalidate() == INVALIDATE_OK)
            .collect(Collectors.toList());
    Assert.assertEquals(unlockedBuffers.stream().mapToLong(LlapCacheableBuffer::getMemoryUsage).sum(), policy.purge());
  }

  @Test public void testPurgeWhenAllUnlocked() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages);
    Collections.shuffle(access, new Random(SEED));
    policy = new LowLevelLockFreeFifoCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    policy.purge();
    Assert.assertEquals(defaultEvictionListener.evicted.size(), numPages);
    for (int i = 0; i < access.size(); i++) {
      Assert.assertEquals(defaultEvictionListener.evicted.get(i), access.get(i));
    }
  }

  @Test public void testPurgeWhenAllLocked() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages, i -> false);
    Collections.shuffle(access, new Random(SEED));
    policy = new LowLevelLockFreeFifoCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    Assert.assertEquals(0, policy.purge());
  }

  private class EvictionTracker implements EvictionListener {
    public List<LlapCacheableBuffer> evicted = new ArrayList<>();

    @Override public void notifyEvicted(LlapCacheableBuffer buffer) {
      evicted.add(buffer);
    }
  }
}