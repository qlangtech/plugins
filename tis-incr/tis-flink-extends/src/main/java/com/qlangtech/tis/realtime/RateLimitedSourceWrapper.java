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

import com.qlangtech.tis.datax.DataXName;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Objects;

/**
 * 实现源端流控
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 10:52
 **/
public class RateLimitedSourceWrapper<T, SplitT extends SourceSplit, EnumChkT> implements Source<T, SplitT, EnumChkT> {
    private final Source<T, SplitT, EnumChkT> delegateSource;
    private final RateLimiterStrategy rateStrategy;
    private final DataXName dataXName;

    public RateLimitedSourceWrapper(DataXName dataXName, Source<T, SplitT, EnumChkT> delegateSource, RateLimiterStrategy rateStrategy) {
        this.delegateSource = Objects.requireNonNull(delegateSource, "delegateSource can not be null");
        this.rateStrategy = Objects.requireNonNull(rateStrategy, "rateStrategy can not be null");
        this.dataXName = dataXName;
    }

    @Override
    public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
        SourceReader<T, SplitT> reader = delegateSource.createReader(readerContext);

        RateLimiter rateLimiter = rateStrategy.createRateLimiter(readerContext.currentParallelism());
        RateLimiterRegistry.registerRateLimiter(dataXName.getPipelineName(), rateLimiter);
        return new RateLimitedSourceReader<>(reader, rateLimiter);
    }

    @Override
    public Boundedness getBoundedness() {
        return delegateSource.getBoundedness();
    }

    @Override
    public SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext) throws Exception {
        return delegateSource.createEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception {
        return delegateSource.restoreEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return delegateSource.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {
        return delegateSource.getEnumeratorCheckpointSerializer();
    }


}
