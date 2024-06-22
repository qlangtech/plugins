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
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
import com.qlangtech.tis.plugins.incr.flink.cdc.ReocrdTransformerMapper;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-20 17:15
 **/
public class RowTransformerMapper extends ReocrdTransformerMapper<Row> {


    public RowTransformerMapper(List<FlinkCol> srcCols //
            , RecordTransformerRules transformerRules //
    ) { //
        super(srcCols, transformerRules);
    }


    @Override
    protected AbstractTransformerRecord<Row> createDelegate(Row row) {
        return new TransformerRow(row);
    }
}
