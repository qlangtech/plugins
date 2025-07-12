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

package com.qlangtech.tis.realtime;

import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-10 09:08
 **/
public class RateLimiterRegistry {
    private static final Map<String, AtomicReference<RateLimiter>> RATE_REGISTRY = new ConcurrentHashMap<>();
    // 注册限流器
    public static void registerRateLimiter(String sourceUid,  RateLimiter rateStrategy) {
        RATE_REGISTRY.computeIfAbsent(sourceUid, k -> new AtomicReference<>(rateStrategy));
    }

    // 更新限流值
    public static void updateRate(String sourceUid, RateLimiter newRate) {
        AtomicReference<RateLimiter> ref = RATE_REGISTRY.get(sourceUid);
        if (ref != null) {
            ref.set(newRate);
        }
    }

    // 获取当前限流值
    public static RateLimiter getCurrentRate(String sourceUid) {
        AtomicReference<RateLimiter> ref = RATE_REGISTRY.get(sourceUid);
        return (ref != null) ? ref.get() : null;
    }
}
