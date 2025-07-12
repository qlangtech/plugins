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

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 16:49
 **/
public class HttpSourceReader extends SourceReaderBase<IncrRateControllerCfgDTO, IncrRateControllerCfgDTO, HttpSourceSplit, Object> {

    // private final String apiUrl;
    // private final Duration pollInterval;
//    private HttpClient httpClient;
//    private HttpSourceSplit currentSplit;
//    private final BlockingQueue<String> recordsQueue = new ArrayBlockingQueue<>(100);
//    private volatile boolean running = true;

    public HttpSourceReader(DataXName dataXName, SourceReaderContext context) {

        // SplitFetcherManager<E, SplitT> splitFetcherManager, RecordEmitter<E, T, SplitStateT> recordEmitter, Configuration config, SourceReaderContext context

        super(
                // new FutureCompletingBlockingQueue<>(20), // 元素队列
                new SingleThreadFetcherManager<IncrRateControllerCfgDTO, HttpSourceSplit>(
                        // 每分钟访问10次
                        () -> new HttpSplitReader(dataXName)), // 错误处理
                new HttpRecordEmitter(),
                new Configuration(),
                Objects.requireNonNull(context, "context can not be null")); // 上下文
//        this.apiUrl = apiUrl;
//        this.pollInterval = pollInterval;
    }


    @Override
    public void close() throws Exception {
//        running = false;
//        if (httpClient != null) {
//            httpClient.close();
//        }
    }

    @Override
    protected Object initializedState(HttpSourceSplit split) {
        return new Object();
    }


    @Override
    protected HttpSourceSplit toSplitType(String splitId, Object splitState) {
        return new HttpSourceSplit(splitId);
    }

    @Override
    protected void onSplitFinished(Map finishedSplitIds) {

    }
}
