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

import com.qlangtech.plugins.incr.flink.cdc.DTO2RowMapper;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.sql.parser.tuple.creator.IStreamIncrGenerateStrategy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 将源DataStream 转成Table
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-18 11:50
 **/
public abstract class TableRegisterFlinkSourceHandle extends BasicFlinkSourceHandle {

    @Override
    protected void processTableStream(StreamExecutionEnvironment env
            , Map<String, DTOStream> tab2OutputTag, SinkFuncs sinkFunction) {

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());


        for (Map.Entry<String, DTOStream> entry : tab2OutputTag.entrySet()) {
            this.registerTable(tabEnv, entry.getKey(), entry.getValue());
        }

        this.executeSql(tabEnv);
    }


    abstract protected void executeSql(StreamTableEnvironment tabEnv);

    @Override
    protected List<FlinkCol> getTabColMetas(TargetResName dataxName, String tabName) {
        return getAllTabColsMeta(dataxName, tabName);
    }

    protected void registerTable(StreamTableEnvironment tabEnv
            , String tabName, DTOStream dtoDataStream) {
        Schema.Builder scmBuilder = Schema.newBuilder();
        List<FlinkCol> cols = dtoDataStream.cols;
        String[] fieldNames = new String[cols.size()];
        TypeInformation<?>[] types = new TypeInformation<?>[cols.size()];
        int i = 0;

        for (FlinkCol col : cols) {
            scmBuilder.column(col.name, col.type);
            // TypeConversions.fromDataTypeToLegacyInfo()
            types[i] = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(col.type);
            fieldNames[i++] = col.name;

        }
        List<String> pks = cols.stream().filter((c) -> c.isPk()).map((c) -> c.name).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(pks)) {
            scmBuilder.primaryKey(pks);
        }
        Schema schema = scmBuilder.build();

        TypeInformation<Row> outputType = Types.ROW_NAMED(fieldNames, types);
        DataStream<Row> rowStream = dtoDataStream.getStream()
                .map(new DTO2RowMapper(cols), outputType).name(tabName).uid("uid_" + tabName);

        Table table = tabEnv.fromChangelogStream(rowStream, schema, ChangelogMode.all());
        tabEnv.createTemporaryView(tabName + IStreamIncrGenerateStrategy.IStreamTemplateData.KEY_STREAM_SOURCE_TABLE_SUFFIX, table);


    }

}
