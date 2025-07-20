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

import com.google.common.util.concurrent.RateLimiter;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.manage.common.TaskSoapUtils;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 18:53
 **/
public class HttpSplitReader implements SplitReader<IncrRateControllerCfgDTO, HttpSourceSplit> {


    private static final Logger logger = LoggerFactory.getLogger(HttpSplitReader.class);
    //  private final Duration pollInterval;
    // private final BlockingQueue<String> recordsQueue = new ArrayBlockingQueue<>(100);
    // private volatile boolean running = true;
//    private HttpClient httpClient;
//    private List<HttpSourceSplit> assignedSplits = new ArrayList<>();

    private final RateLimiter requestRateLimit = RateLimiter.create(0.1);

    // 限流配置
    private final DataXName dataXName;
    // 状态跟踪
//    private AtomicLong lastFetchTime = new AtomicLong(0);
//    private AtomicLong requestCount = new AtomicLong(0);
//    private long lastResetTime = System.currentTimeMillis();

    private Long lastModified;

    public HttpSplitReader(DataXName dataXName) {
        this.dataXName = dataXName;
    }


    @Override
    public void handleSplitsChanges(SplitsChange<HttpSourceSplit> splitsChanges) {
        // 处理新增分片
//        splitsChanges.splits().forEach(split -> {
//            if (!assignedSplits.contains(split)) {
//                assignedSplits.add(split);
//                System.out.println("Added split: " + split.splitId());
//            }
//        });
    }

    @Override
    public RecordsWithSplitIds<IncrRateControllerCfgDTO> fetch() throws IOException {
        applyRateLimit();

        IncrRateControllerCfgDTO rateLimit
                = TaskSoapUtils.getIncrRateLimitCfg(dataXName, this.lastModified == null ? 0 : this.lastModified);

        if (rateLimit == null || this.lastModified == null) {
            if (rateLimit != null) {
                // 保证第一次不往流中下发
                this.lastModified = rateLimit.getLastModified();
            }
            SimpleRecords records = new SimpleRecords(null);
            records.emitted = true;
            logger.info("skip rateCfg");
            return records;
        } else {
            SimpleRecords records = new SimpleRecords(rateLimit);
            this.lastModified = rateLimit.getLastModified();
            logger.info("send rateCfg:{},pause:{}", rateLimit.getControllerType());
            return records;
        }
    }

    private void applyRateLimit() {
        // 每分钟重置计数器
        requestRateLimit.acquire();
    }


    @Override
    public void wakeUp() {
        // 中断阻塞操作
        // running = false;
    }

    @Override
    public void close() throws IOException {
        // running = false;
//        if (httpClient != null) {
//            httpClient.close();
//        }
    }

    //    // 后台轮询线程
//    public void start() {
//        new Thread(() -> {
//            while (running) {
//                try {
//                    HttpGet request = new HttpGet(apiUrl);
//                    HttpResponse response = httpClient.execute(request);
//                    String data = EntityUtils.toString(response.getEntity());
//                    recordsQueue.put(data);
//                    Thread.sleep(pollInterval.toMillis());
//                } catch (Exception e) {
//                    if (running) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }).start();
//    }
    // 简单记录实现
    private static class SimpleRecords implements RecordsWithSplitIds<IncrRateControllerCfgDTO> {
        private final IncrRateControllerCfgDTO record;
        private boolean emitted = false;

        public SimpleRecords(IncrRateControllerCfgDTO record) {
            this.record = record;
        }

        @Override
        public String nextSplit() {
            return emitted ? null : HttpSourceSplit.KEY_SPLIT_ID;
        }

        @Override
        public IncrRateControllerCfgDTO nextRecordFromSplit() {
            if (emitted) {
                return null;
            }
            emitted = true;
            return record;
        }

        @Override
        public Set<String> finishedSplits() {
            // return emitted ? Collections.singleton(HttpSourceSplit.KEY_SPLIT_ID) : ;
            return Collections.emptySet();
        }
    }
}
