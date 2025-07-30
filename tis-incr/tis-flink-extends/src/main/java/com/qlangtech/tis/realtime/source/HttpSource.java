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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 16:44
 **/
public class HttpSource implements Source<IncrRateControllerCfgDTO, HttpSourceSplit, Collection<HttpSourceSplit>> {


    private final DataXName dataXName;
    // private final Duration pollInterval;

    public HttpSource(DataXName dataXName) {
        this.dataXName = Objects.requireNonNull(dataXName, "dataXName can not be null");
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<IncrRateControllerCfgDTO, HttpSourceSplit> createReader(SourceReaderContext context) {
        return new HttpSourceReader(dataXName, context);
    }

    @Override
    public SplitEnumerator<HttpSourceSplit, Collection<HttpSourceSplit>> createEnumerator(
            SplitEnumeratorContext<HttpSourceSplit> enumContext) {
        // 创建单个分片
        HttpSourceSplit split = new HttpSourceSplit(HttpSourceSplit.KEY_SPLIT_ID);
        return new HttpSourceEnumerator(Collections.singletonList(split), enumContext);
    }

    @Override
    public SplitEnumerator<HttpSourceSplit, Collection<HttpSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<HttpSourceSplit> enumContext,
            Collection<HttpSourceSplit> checkpoint) {
        //  return new HttpSourceEnumerator(new ArrayList<>(checkpoint), enumContext);
        return this.createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<HttpSourceSplit> getSplitSerializer() {
        return new HttpSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<HttpSourceSplit>> getEnumeratorCheckpointSerializer() {
        return new HttpSplitsSerializer();
    }

//    @Override
//    public TypeInformation<String> getProducedType() {
//        return Types.STRING;
//    }
}