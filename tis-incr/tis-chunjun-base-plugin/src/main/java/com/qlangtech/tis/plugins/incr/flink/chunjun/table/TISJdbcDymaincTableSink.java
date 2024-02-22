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

package com.qlangtech.tis.plugins.incr.flink.chunjun.table;

import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-02-21 11:30
 * @see com.dtstack.chunjun.connector.jdbc.sink.JdbcDynamicTableSink
 **/
public class TISJdbcDymaincTableSink implements DynamicTableSink {

    private final BasicTISSinkFactory.RowDataSinkFunc rowDataSinkFunc;
    private final IEndTypeGetter.EndType endType;

    public TISJdbcDymaincTableSink(IEndTypeGetter.EndType endType, BasicTISSinkFactory.RowDataSinkFunc rowDataSinkFunc) {
        this.rowDataSinkFunc = rowDataSinkFunc;
        this.endType = endType;
    }
//    public TISJdbcDymaincTableSink(JdbcConf jdbcConf, JdbcDialect jdbcDialect
//            , TableSchema tableSchema, JdbcOutputFormatBuilder builder) {
//        super(jdbcConf, jdbcDialect, tableSchema, builder);
//    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        //return super.getSinkRuntimeProvider(context);

        return rowDataSinkFunc.getSinkRuntimeProvider();

        // return SinkFunctionProvider.of(rowDataSinkFunc.getSinkFunction(), rowDataSinkFunc.getSinkTaskParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new TISJdbcDymaincTableSink(endType, rowDataSinkFunc);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + endType.getVal();
    }
}
