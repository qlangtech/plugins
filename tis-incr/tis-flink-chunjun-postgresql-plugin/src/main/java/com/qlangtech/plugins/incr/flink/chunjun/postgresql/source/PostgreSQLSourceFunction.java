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

package com.qlangtech.plugins.incr.flink.chunjun.postgresql.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.postgresql.source.PostgresqlSourceFactory;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.qlangtech.plugins.incr.flink.chunjun.postgresql.dialect.TISPostgresqlDialect;
import com.qlangtech.tis.plugins.incr.flink.chunjun.source.ChunjunSourceFunction;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-27 08:32
 **/
public class PostgreSQLSourceFunction extends ChunjunSourceFunction {


    public PostgreSQLSourceFunction(ChunjunPostgreSQLSourceFactory sourceFactory) {
        super(sourceFactory);
    }


    protected JdbcSourceFactory createChunjunSourceFactory(
            SyncConf conf, BasicDataSourceFactory sourceFactory, AtomicReference<SourceFunction<RowData>> sourceFunc) {
        return new ExtendPostgresqlSourceFactory(conf, null, sourceFactory) {
            protected DataStream<RowData> createInput(
                    InputFormat<RowData, InputSplit> inputFormat, String sourceName) {
                Preconditions.checkNotNull(sourceName);
                Preconditions.checkNotNull(inputFormat);
                DtInputFormatSourceFunction<RowData> function =
                        new DtInputFormatSourceFunction<>(inputFormat, getTypeInformation());
                sourceFunc.set(function);
                return null;
            }
        };
    }

    private static class ExtendPostgresqlSourceFactory extends PostgresqlSourceFactory {
        private final DataSourceFactory dataSourceFactory;

        public ExtendPostgresqlSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env, DataSourceFactory dataSourceFactory) {
            super(syncConf, env, new TISPostgresqlDialect(syncConf));
            this.fieldList = syncConf.getReader().getFieldList();
            this.dataSourceFactory = dataSourceFactory;
        }

        @Override
        protected JdbcInputFormatBuilder getBuilder() {
            return new JdbcInputFormatBuilder(new TISPostgresqlInputFormat(dataSourceFactory));
        }
    }

//    protected String parseType(ISelectedTab.ColMeta cm) {
//        return typeMapper(cm);
//    }

//    public static String typeMapper(ISelectedTab.ColMeta cm) {
//        // https://dtstack.github.io/chunjun/documents/7d23239f-9f24-5889-af9c-fc412d788060
//        return cm.getType().accept(new DataType.TypeVisitor<String>() {
//            @Override
//            public String bigInt(DataType type) {
//                return "BIGINT";
//            }
//
//            @Override
//            public String doubleType(DataType type) {
//                return "DOUBLE PRECISION";
//            }
//
//            @Override
//            public String dateType(DataType type) {
//                return "DATE";
//            }
//
//            @Override
//            public String timestampType(DataType type) {
//                return "TIMESTAMP";
//            }
//
//            @Override
//            public String bitType(DataType type) {
//                return "BOOL";
//            }
//
//            @Override
//            public String blobType(DataType type) {
//                return "BYTEA";
//            }
//
//            @Override
//            public String varcharType(DataType type) {
//                return "VARCHAR";
//            }
//
//            @Override
//            public String intType(DataType type) {
//                return "INT";
//            }
//
//            @Override
//            public String floatType(DataType type) {
//                return "FLOAT";
//            }
//
//            @Override
//            public String decimalType(DataType type) {
//                return "DECIMAL";
//            }
//
//            @Override
//            public String timeType(DataType type) {
//                return "TIME";
//            }
//
//            @Override
//            public String tinyIntType(DataType dataType) {
//                return smallIntType(null);
//            }
//
//            @Override
//            public String smallIntType(DataType dataType) {
//                return "SMALLINT";
//            }
//        });
//    }
}
