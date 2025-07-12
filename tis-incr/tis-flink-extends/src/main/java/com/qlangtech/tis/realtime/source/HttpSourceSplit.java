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

package com.qlangtech.tis.realtime.source;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 16:45
 **/
public class HttpSourceSplit implements SourceSplit {
    public static final String KEY_SPLIT_ID = "http-split-0";
    private final String id;
    private long lastPollTime = 0; // 用于记录上次轮询时间

    public HttpSourceSplit(String id) {
        this.id = id;
    }

    @Override
    public String splitId() {
        return id;
    }

    public void updatePollTime() {
        this.lastPollTime = System.currentTimeMillis();
    }

    public long getLastPollTime() {
        return lastPollTime;
    }

    public void setLastPollTime(long lastPollTime) {
        this.lastPollTime = lastPollTime;
    }
}
