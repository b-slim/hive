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

package org.apache.hive.common.util;

import org.apache.hadoop.hive.common.Pool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class FixedSizedObjectPoolTest {

  private FixedSizedObjectPool<AtomicInteger> fixedSizedObjectPool;


  @Before public void setUp() throws Exception {
    fixedSizedObjectPool = new FixedSizedObjectPool<>(32, new Pool.PoolObjectHelper<AtomicInteger>() {
      @Override public AtomicInteger create() {
        //System.out.println("creating object by " + Thread.currentThread().getId());
        return new AtomicInteger(0);
      }

      @Override public void resetBeforeOffer(AtomicInteger atomicInteger) {
        //System.out.println("resting object by " + Thread.currentThread().getId());
        Assert.assertTrue(atomicInteger.compareAndSet(1, 0));
      }
    });

  }

  @After public void tearDown() throws Exception {
  }

  private void testRunnable(FixedSizedObjectPool<AtomicInteger> objectPool, int numRuns) throws InterruptedException {
    long sleepTime = Thread.currentThread().getId() % 10;
    System.out.println("running with thread id " + Thread.currentThread().getId() + " sleep time is " + sleepTime);
    Thread.sleep(sleepTime);
    ArrayList<AtomicInteger> vals = new ArrayList((int) sleepTime);
    for (int i = 0; i < numRuns; i++) {
      for (int j = 0; j < sleepTime; j++) {
        AtomicInteger val = objectPool.take();
        assertTrue(val.compareAndSet(0, 1));
        vals.add(val);
      }

      for (int j = 0; j < sleepTime; j++) {
        objectPool.offer(vals.get(j));
      }
      vals.clear();
    }
    System.out.println("DONE thread id" + Thread.currentThread().getId());
  }

  @Test public void testAssertTT() throws InterruptedException {

    int numRuns = 2000000;
    Runnable runnable = () -> {
      try {
        testRunnable(fixedSizedObjectPool, numRuns);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
    testAssertConcurrent("Test Failed",
        IntStream.range(0, 48).mapToObj(i -> runnable).collect(Collectors.toList()),
        150);
  }

  public void testAssertConcurrent(String message, List<? extends Runnable> runnables, int maxTimeoutSeconds)
      throws InterruptedException {
    final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    try {
      final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
      final CountDownLatch afterInitBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      for (final Runnable submittedTestRunnable : runnables) {
        threadPool.submit(new Runnable() {
          public void run() {
            allExecutorThreadsReady.countDown();
            try {
              afterInitBlocker.await();
              submittedTestRunnable.run();
            } catch (final Throwable e) {
              exceptions.add(e);
            } finally {
              allDone.countDown();
            }
          }
        });
      }
      // wait until all threads are ready
      assertTrue("Timeout initializing threads! Perform long lasting initializations before passing runnables to "
          + "assertConcurrent;", allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));
      // start all test runners
      afterInitBlocker.countDown();
      assertTrue(message + "; timeout! More than; " + maxTimeoutSeconds + ";seconds;",
          allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
    } finally {
      threadPool.shutdownNow();
    }
    assertTrue(message + ";failed with exception(s);" + exceptions, exceptions.isEmpty());
  }
}