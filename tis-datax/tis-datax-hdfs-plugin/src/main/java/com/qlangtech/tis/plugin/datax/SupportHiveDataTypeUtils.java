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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.plugin.writer.hdfswriter.SupportHiveDataType;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-09 19:54
 **/
public class SupportHiveDataTypeUtils {

    private SupportHiveDataTypeUtils() {
    }

    static SupportHiveDataType convert2HiveType(ColumnMetaData.DataType type) {
        Objects.requireNonNull(type, "param type can not be null");

        return type.accept(new ColumnMetaData.TypeVisitor<SupportHiveDataType>() {
            @Override
            public SupportHiveDataType intType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.INT;
            }

            @Override
            public SupportHiveDataType floatType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.FLOAT;
            }

            @Override
            public SupportHiveDataType decimalType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.DOUBLE;
            }

            @Override
            public SupportHiveDataType timeType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.TIMESTAMP;
            }

            @Override
            public SupportHiveDataType tinyIntType(ColumnMetaData.DataType dataType) {
                return SupportHiveDataType.TINYINT;
            }

            @Override
            public SupportHiveDataType smallIntType(ColumnMetaData.DataType dataType) {
                return SupportHiveDataType.SMALLINT;
            }

            @Override
            public SupportHiveDataType longType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.BIGINT;
            }

            @Override
            public SupportHiveDataType doubleType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.DOUBLE;
            }

            @Override
            public SupportHiveDataType dateType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.DATE;
            }

            @Override
            public SupportHiveDataType timestampType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.TIMESTAMP;
            }

            @Override
            public SupportHiveDataType bitType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.BOOLEAN;
            }

            @Override
            public SupportHiveDataType blobType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.STRING;
            }

            @Override
            public SupportHiveDataType varcharType(ColumnMetaData.DataType type) {
                return SupportHiveDataType.STRING;
            }
        });
    }
}
