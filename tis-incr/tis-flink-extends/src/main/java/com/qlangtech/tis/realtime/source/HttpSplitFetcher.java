///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.qlangtech.tis.realtime.source;
//
//import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
//import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
//
//import java.util.Set;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-07-06 16:51
// **/
//public class HttpSplitFetcher extends SplitFetcher<String, HttpSourceSplit> {
//
//    private final String apiUrl;
//    private final BlockingQueue<String> recordsQueue;
//
//    public HttpSplitFetcher(String apiUrl, BlockingQueue<String> recordsQueue) {
//        this.apiUrl = apiUrl;
//        this.recordsQueue = recordsQueue;
//    }
//
//    @Override
//    public RecordsWithSplitIds<String> fetch() {
//        try {
//            // 从队列中获取数据
//            String record = recordsQueue.poll(100, TimeUnit.MILLISECONDS);
//            if (record != null) {
//                return new RecordsWithSplitIds<String>() {
//                    private boolean emitted = false;
//
//                    @Override
//                    public String nextSplit() {
//                        if (!emitted) {
//                            emitted = true;
//                            return "http-split-0"; // 分片ID
//                        }
//                        return null;
//                    }
//
//                    @Override
//                    public String nextRecordFromSplit() {
//                        return emitted ? record : null;
//                    }
//
//                    @Override
//                    public Set<String> finishedSplits() {
//                        // 无操作
//                        return null;
//                    }
//                };
//            }
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//        return null;
//    }
//
//    @Override
//    public void close() {
//        // 清理资源
//    }
//}
