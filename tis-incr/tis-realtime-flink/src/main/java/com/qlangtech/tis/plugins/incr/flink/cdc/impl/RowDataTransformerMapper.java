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

package com.qlangtech.tis.plugins.incr.flink.cdc.impl;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
import com.qlangtech.tis.plugins.incr.flink.cdc.ReocrdTransformerMapper;
import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-20 17:09
 **/
public class RowDataTransformerMapper extends ReocrdTransformerMapper<RowData> {
    private final List<FlinkCol> originColsWithContextParamsFlinkCol;
    private final int delegateArity;

    public RowDataTransformerMapper(SelectedTableTransformerRules triple, int delegateArity) {
        super(FlinkCol.getAllTabColsMeta(triple.overwriteColsWithContextParams() //rule.overwriteCols(table.getCols())
                , Objects.requireNonNull(triple.getSourceFlinkColCreator(), "flinkColCreator")), triple.getTransformerRules());
        this.originColsWithContextParamsFlinkCol = triple.originColsWithContextParamsFlinkCol();
        this.delegateArity = delegateArity;
    }


    @Override
    protected AbstractTransformerRecord<RowData> createDelegate(RowData row) {
        return new TransformerRowData(row, this.cols, this.originColsWithContextParamsFlinkCol, this.delegateArity);
    }
}
