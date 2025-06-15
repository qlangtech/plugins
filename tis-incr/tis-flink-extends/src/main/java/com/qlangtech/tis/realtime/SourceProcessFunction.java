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

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * 负责给主数据流打标，提供给后续流程 {@link com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream} 实现分流
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-27 10:38
 **/
public abstract class SourceProcessFunction<RECORD_TYPE> extends ProcessFunction<RECORD_TYPE, RECORD_TYPE> {
    private final Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag;

    public SourceProcessFunction(Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag) {
        this.tab2OutputTag = tab2OutputTag;
    }

    /**
     * 在主流中为每个表打标签
     *
     * @param in   The input value.
     * @param ctx  A {@link Context} that allows querying the timestamp of the element and getting a
     *             {@link org.apache.flink.streaming.api.TimerService} for registering timers and querying the time. The context is only
     *             valid during the invocation of this method, do not store it.
     * @param _out The collector for returning result values.
     * @throws Exception
     * @see com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream#addStream collected in it
     */
    @Override
    public void processElement(RECORD_TYPE in, Context ctx, Collector<RECORD_TYPE> _out) throws Exception {
        //side_output: https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/side_output.html
        final String tabName = getTableName(in);// in.getTableName();
        OutputTag<RECORD_TYPE> outputTag = tab2OutputTag.get(tabName);
        if (outputTag == null) {
            throw new IllegalStateException("target table:" + tabName + " can not find relevant in tab2OutputTag");
        }
        ctx.output(outputTag, in);
    }

    protected abstract String getTableName(RECORD_TYPE record);


}
