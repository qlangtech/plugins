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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.Schema;


import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-21 14:13
 **/
public class RowUtils {

    private static TypeInformation<Row> getRowOutputType(List<String> primaryKeys, List<FlinkCol> cols) {

        String[] fieldNames = new String[cols.size()];
        TypeInformation<?>[] types = new TypeInformation<?>[cols.size()];
        int i = 0;
        DataType colType = null;
        for (FlinkCol col : cols) {
            colType = createFlinkColType(primaryKeys, col);
            // scmBuilder.column(col.name, colType);
            // TypeConversions.fromDataTypeToLegacyInfo()

            types[i] = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(colType);
            fieldNames[i++] = col.name;
        }
        TypeInformation<Row> outputType = Types.ROW_NAMED(fieldNames, types);
        return outputType;
    }

    private static org.apache.flink.table.api.Schema createSchema(List<String> primaryKeys, List<FlinkCol> cols) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            throw new IllegalStateException("pks can not be empty");
        }
        org.apache.flink.table.api.Schema.Builder scmBuilder = org.apache.flink.table.api.Schema.newBuilder();
        //  String[] fieldNames = new String[cols.size()];
        // TypeInformation<?>[] types = new TypeInformation<?>[cols.size()];
        int i = 0;
        DataType colType = null;
        for (FlinkCol col : cols) {
            colType = createFlinkColType(primaryKeys, col);
            scmBuilder.column(col.name, colType);
            // TypeConversions.fromDataTypeToLegacyInfo()

            // types[i] = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(colType);
            //    fieldNames[i++] = col.name;

        }
        List<String> pks = primaryKeys;
        if (CollectionUtils.isEmpty(pks)) {
            throw new IllegalStateException("pks can not be empty");
        }
        scmBuilder.primaryKey(pks);
        org.apache.flink.table.api.Schema schema = scmBuilder.build();
        return schema;
    }

    public static DataType createFlinkColType(List<String> primaryKeys, FlinkCol col) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            throw new IllegalArgumentException("primaryKeys can not be empty");
        }
        DataType type = col.type;
        if (primaryKeys.contains(col.name)) {
            if (type.getLogicalType().isNullable()) {
                type = type.notNull();// new AtomicDataType(type.getLogicalType().copy(false));
            }
        }
        return type;
    }

    public static Pair<TypeInformation<Row>, Schema> outputTypeSchema(List<FlinkCol> cols, List<String> primaryKeys) {
        return Pair.of(RowUtils.getRowOutputType(primaryKeys, cols), RowUtils.createSchema(primaryKeys, cols));
    }
}
