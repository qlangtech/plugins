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

package com.qlangtech.tis.plugins.incr.flink.cdc;

import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-15 10:26
 **/
public enum DTOConvertTo {
    RowData((flinkCol) -> {
        //  return flinkCol.rowDataProcess.apply(val);
        return flinkCol.rowDataProcess;
    }, BasicFlinkDataMapper.STRING_FUNC_PROCESS.getLeft()),
    Row((flinkCol) -> {
        //  return flinkCol.rowDataProcess.apply(val);
        return flinkCol.rowProcess;
    }, BasicFlinkDataMapper.STRING_FUNC_PROCESS.getMiddle()),
    FlinkCDCPipelineEvent((flinkCol) -> {
        return flinkCol.flinkCDCPipelineEventProcess;
    }, BasicFlinkDataMapper.STRING_FUNC_PROCESS.getRight());

    private final java.util.function.Function<FlinkCol, BiFunction> targetValGetter;
    public final BiFunction stringValProcessor;

    private DTOConvertTo(java.util.function.Function<FlinkCol, BiFunction> targetValGetter, BiFunction stringValProcessor) {
        // BasicFlinkDataMapper
        this.targetValGetter = targetValGetter;
        this.stringValProcessor = stringValProcessor;
    }

    public Object processVal(FlinkCol flinkCol, Object val) {
        if (val == null) {
            return null;
        }
        return getValGetter(flinkCol).apply(val);
    }

    public BiFunction getValGetter(FlinkCol flinkCol) {
        BiFunction valGetter = this.targetValGetter.apply(Objects.requireNonNull(flinkCol, "flinkCol can not be null"));
        return valGetter;
    }

    public String toString(FlinkCol flinkCol, Object val) {
        if (val == null) {
            return null;
        }
        // 利用flink中的 RowData ，Row中的值转成String，特别针对Date，Timestamp类型的值需要定制化toStringVal方法
        return targetValGetter.apply(flinkCol).toStringVal(val);
    }
}
