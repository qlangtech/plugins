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

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Maps;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableMeataCreator;
import com.qlangtech.tis.datax.IStreamTableMeta;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-20 14:04
 **/
public class TestStubSinkFactory extends TISSinkFactory implements IStreamTableMeataCreator.ISinkStreamMetaCreator {
    protected final List<ColMeta> cols;

    public TestStubSinkFactory(List<ColMeta> cols) {
        this.cols = cols;
    }

    @Override
    public IStreamTableMeta getStreamTableMeta(String tableName) {
        return new IStreamTableMeta() {
            @Override
            public List<IColMetaGetter> getColsMeta() {
                if (CollectionUtils.isEmpty(cols)) {
                    throw new IllegalStateException("cols can not be null");
                }
                return cols.stream().map(c -> c.meta).collect(Collectors.toList());
            }
        };
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TableAlias, SinkFunction<DTO>> createSinkFunction(
            IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator) {
        Map<TableAlias, SinkFunction<DTO>> createdSinkFunc = Maps.newHashMap();
        return createdSinkFunc;
    }

//        public CreatedSinkFunction<SinkFunction<DTO>, FlinkCol> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator<FlinkCol> flinkColCreator) {
//
//            CreatedSinkFunction<SinkFunction<DTO>, FlinkCol> createdSinkFunc = new CreatedSinkFunction<>();
//
//            return createdSinkFunc;
//        }
}
