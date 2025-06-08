/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.realtime;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.datax.TableAlias;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-24 07:58
 **/
abstract class AbstractTabSinkFuncV1<
        SINOPERATOR extends SinkFunction<SINK_TRANSFER_OBJ>
        , ADD_SINK_RESULT extends DataStreamSink<SINK_TRANSFER_OBJ>
        , SINK_TRANSFER_OBJ>
        extends TabSinkFunc<SINOPERATOR, ADD_SINK_RESULT, SINK_TRANSFER_OBJ> {

    private transient List<String> primaryKeys;
    protected transient final TableAlias tab;

    protected final List<FlinkCol> sinkColsMeta;
    protected final List<FlinkCol> sourceColsMeta;
    protected transient final Optional<SelectedTableTransformerRules> transformers;



    public AbstractTabSinkFuncV1(TableAlias tab, List<String> primaryKeys, SINOPERATOR sinkFunction
            , List<FlinkCol> sourceColsMeta, List<FlinkCol> sinkColsMeta, int sinkTaskParallelism, Optional<SelectedTableTransformerRules> transformerOpt) {
        super(sinkFunction, sinkTaskParallelism);
        this.primaryKeys = primaryKeys;
        this.tab = tab;
        if (CollectionUtils.isEmpty(sinkColsMeta)) {
            throw new IllegalArgumentException("colsMeta can not be empty");
        }
        this.sinkColsMeta = sinkColsMeta;
        this.sourceColsMeta = sourceColsMeta;
        this.transformers = transformerOpt;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<FlinkCol> getSinkColsMeta() {
        return this.sinkColsMeta;
    }

    public List<FlinkCol> getSourceColsMeta() {
        return this.sourceColsMeta;
    }
}
