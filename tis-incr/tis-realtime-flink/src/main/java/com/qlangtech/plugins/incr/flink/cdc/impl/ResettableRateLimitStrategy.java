/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.cdc.impl;

import com.qlangtech.tis.plugin.incr.TISRateLimiter.IResettableRateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 12:53
 **/
public class ResettableRateLimitStrategy implements RateLimiterStrategy {

    private final Integer permitsPerSecond;

    public ResettableRateLimitStrategy(Integer permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    @Override
    public RateLimiter createRateLimiter(int parallelism) {
        return new ResettableGuavaRateLimiter(permitsPerSecond / parallelism, parallelism);
    }


    private static class ResettableGuavaRateLimiter implements org.apache.flink.api.connector.source.util.ratelimit.RateLimiter, IResettableRateLimiter {
        private volatile com.google.common.util.concurrent.RateLimiter guavaLimiter;
        private final Executor limiter =
                Executors.newSingleThreadExecutor(new ExecutorThreadFactory("flink-rate-limiter"));
        private final int parallelism;
        protected AtomicBoolean pauseFlag = new AtomicBoolean(false);
        private static final Logger logger = LoggerFactory.getLogger(ResettableGuavaRateLimiter.class);

        public ResettableGuavaRateLimiter(double permitsPerSecond, int parallelism) {
            this.guavaLimiter = com.google.common.util.concurrent.RateLimiter.create(permitsPerSecond);
            if (parallelism < 1) {
                throw new IllegalArgumentException("parallelism can not small than 1");
            }
            this.parallelism = parallelism;
        }

        public void shallPause() throws InterruptedException {
            if (pauseFlag.get()) {
                synchronized (pauseFlag) {
                    if (pauseFlag.get()) {
                        pauseFlag.wait();
                    }
                }
            }
        }

        /**
         * 重新啟動增量消息
         */
        @Override
        public void resumeConsume() {
            synchronized (this.pauseFlag) {
                if (this.pauseFlag.compareAndSet(true, false)) {
                    this.pauseFlag.notifyAll();
                    logger.info(" execute resume command");
                }
            }
        }

        /**
         * 停止增量接收消息
         */
        @Override
        public void pauseConsume() {
            if (this.pauseFlag.compareAndSet(false, true)) {
                logger.info(" execute pause command");
            }
        }

        @Override
        public CompletionStage<Void> acquire() {
            return CompletableFuture.runAsync(() -> {
                try {
                    shallPause();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                guavaLimiter.acquire();
            }, limiter);
        }

        /**
         * 核心方法：动态重置速率
         *
         * @param permitsPerSecond
         */
        @Override
        public void resetRate(Integer permitsPerSecond) {
            this.guavaLimiter = com.google.common.util.concurrent.RateLimiter.create(permitsPerSecond / parallelism);
        }
    }
}
