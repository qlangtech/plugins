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

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.dto.DTOStream;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink.Context;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * <TRANSFER_OBJ/> 可以是用：
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-16 12:32
 * @see com.qlangtech.tis.realtime.transfer.DTO
 **/
public abstract class TabSinkFunc<SINK_TRANSFER_OBJ> {

    private transient List<String> primaryKeys;
    protected transient final SinkFunction<SINK_TRANSFER_OBJ> sinkFunction;
    protected transient final TableAlias tab;
    protected transient final int sinkTaskParallelism;
    protected final List<FlinkCol> sinkColsMeta;
    protected final List<FlinkCol> sourceColsMeta;

    public List<FlinkCol> getSinkColsMeta() {
        return this.sinkColsMeta;
    }

    public List<FlinkCol> getSourceColsMeta() {
        return this.sourceColsMeta;
    }

    private transient Pair<String, FilterFunction<SINK_TRANSFER_OBJ>> sourceFilter;
    protected transient final Optional<Triple<RecordTransformerRules, ISelectedTab, IFlinkColCreator<FlinkCol>>> transformers;

    public TabSinkFunc(TableAlias tab, List<String> primaryKeys, SinkFunction<SINK_TRANSFER_OBJ> sinkFunction
            , final List<FlinkCol> sinkColsMeta, int sinkTaskParallelism) {
        this(tab, primaryKeys, sinkFunction, sinkColsMeta, sinkColsMeta, sinkTaskParallelism, Optional.empty());
    }

    /**
     * @param tab
     * @param sinkFunction
     */
    public TabSinkFunc(TableAlias tab, List<String> primaryKeys, SinkFunction<SINK_TRANSFER_OBJ> sinkFunction
            , final List<FlinkCol> sourceColsMeta, final List<FlinkCol> sinkColsMeta, int sinkTaskParallelism
            , Optional<Triple<RecordTransformerRules, ISelectedTab, IFlinkColCreator<FlinkCol>>> transformerOpt) {
        if (CollectionUtils.isEmpty(sinkColsMeta)) {
            throw new IllegalArgumentException("colsMeta can not be empty");
        }
        this.primaryKeys = primaryKeys;
        this.sinkFunction = sinkFunction;
        this.tab = tab;
        if (sinkTaskParallelism < 1) {
            throw new IllegalArgumentException("param sinkTaskParallelism can not small than 1");
        }
        this.sinkTaskParallelism = sinkTaskParallelism;
        this.sinkColsMeta = sinkColsMeta;
        this.sourceColsMeta = sourceColsMeta;
        this.transformers = transformerOpt;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setSourceFilter(String name, FilterFunction<SINK_TRANSFER_OBJ> sourceFilter) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("param name can not be empty");
        }
        if (sourceFilter == null) {
            throw new IllegalArgumentException("param sourceFilter can not be empty");
        }
        this.sourceFilter = Pair.of(name, sourceFilter);
    }

    /**
     * map
     *
     * @param sourceStream
     * @return
     */
    protected abstract DataStream<SINK_TRANSFER_OBJ> streamMap(DTOStream sourceStream);

    public DataStreamSink<SINK_TRANSFER_OBJ> add2Sink(DTOStream sourceStream) {

        DataStream<SINK_TRANSFER_OBJ> source = this.streamMap(sourceStream);

        if (sourceFilter != null) {
            source = source.filter(this.sourceFilter.getRight())
                    .name(this.sourceFilter.getLeft())
                    .setParallelism(this.sinkTaskParallelism);
        }
        if (this.sinkTaskParallelism < 1) {
            throw new IllegalStateException("sinkTaskParallelism can not small than 1");
        }
        return source.addSink(sinkFunction).name(tab.getTo()).setParallelism(this.sinkTaskParallelism);
    }
}
