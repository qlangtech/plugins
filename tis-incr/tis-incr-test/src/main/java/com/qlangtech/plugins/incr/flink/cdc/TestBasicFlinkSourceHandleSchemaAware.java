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

import com.qlangtech.tis.realtime.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-17 16:37
 **/
public class TestBasicFlinkSourceHandleSchemaAware extends TestBasicFlinkSourceHandle {
    private final List<FlinkCol> cols;

    public TestBasicFlinkSourceHandleSchemaAware(String tabName, List<FlinkCol> cols) {
        super(tabName);
        this.cols = cols;
    }

    @Override
    protected void createTemporaryView(Map<String, DTOStream> streamMap) {
        Schema.Builder scmBuilder = Schema.newBuilder();

        String[] fieldNames = new String[cols.size()];
        TypeInformation<?>[] types = new TypeInformation<?>[cols.size()];
        int i = 0;

        for (FlinkCol col : cols) {
            scmBuilder.column(col.name, col.type);
            // TypeConversions.fromDataTypeToLegacyInfo()
            types[i] = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(col.type);
            fieldNames[i++] = col.name;
        }

        scmBuilder.primaryKey(
                cols.stream().filter((c) -> c.isPk())
                        .map((c) -> c.name).collect(Collectors.toList()));

        Schema schema = scmBuilder.build();
        DTOStream dtoDataStream = streamMap.get(tabName);
        TypeInformation<Row> outputType = Types.ROW_NAMED(fieldNames, types);
        DataStream<Row> rowStream = dtoDataStream.getStream().map(new DTO2RowMapper(cols), outputType);

        Table table = tabEnv.fromChangelogStream(rowStream, schema, ChangelogMode.all());
        tabEnv.createTemporaryView(tabName, table);
    }


}
