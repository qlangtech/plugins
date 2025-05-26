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

import com.qlangtech.tis.realtime.dto.DTOStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * <TRANSFER_OBJ/> 可以是用：
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-16 12:32
 * @see com.qlangtech.tis.realtime.transfer.DTO
 **/
public abstract class TabSinkFunc<SINK_OPERATOR, ADD_SINK_RESULT, SINK_TRANSFER_OBJ> {


    protected transient final SINK_OPERATOR sinkFunction;

    protected transient final int sinkTaskParallelism;

    private transient Pair<String, FilterFunction<SINK_TRANSFER_OBJ>> sourceFilter;

    /**
     * @param sinkFunction
     */
    public TabSinkFunc(
            SINK_OPERATOR sinkFunction
            , int sinkTaskParallelism) {


        this.sinkFunction = sinkFunction;
        if (sinkTaskParallelism < 1) {
            throw new IllegalArgumentException("param sinkTaskParallelism can not small than 1");
        }
        this.sinkTaskParallelism = sinkTaskParallelism;
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


    public ADD_SINK_RESULT add2Sink(DTOStream sourceStream) {
        DataStream<SINK_TRANSFER_OBJ> source = this.streamMap(sourceStream);

        if (sourceFilter != null) {
            source = source.filter(this.sourceFilter.getRight())
                    .name(this.sourceFilter.getLeft())
                    .setParallelism(this.sinkTaskParallelism);
        }
        if (this.sinkTaskParallelism < 1) {
            throw new IllegalStateException("sinkTaskParallelism can not small than 1");
        }
        return addSinkToSource(source);
    }

    protected abstract ADD_SINK_RESULT addSinkToSource(DataStream<SINK_TRANSFER_OBJ> source);

}
