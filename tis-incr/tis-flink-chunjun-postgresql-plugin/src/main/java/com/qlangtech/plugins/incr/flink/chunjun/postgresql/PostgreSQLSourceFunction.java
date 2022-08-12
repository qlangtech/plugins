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

package com.qlangtech.plugins.incr.flink.chunjun.postgresql;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.postgresql.source.PostgresqlSourceFactory;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.qlangtech.plugins.incr.flink.chunjun.source.ChunjunSourceFunction;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
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

//    @Override
//    protected JdbcSourceFactory createChunjunSourceFactory(
//            SyncConf conf, BasicDataSourceFactory sourceFactory, AtomicReference<SourceFunction<RowData>> sourceFunc) {
//        return null;
//    }

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
            super(syncConf, env);
            this.fieldList = syncConf.getReader().getFieldList();
            this.dataSourceFactory = dataSourceFactory;
        }

        @Override
        protected JdbcInputFormatBuilder getBuilder() {
            return new JdbcInputFormatBuilder(new TISPostgresqlInputFormat(dataSourceFactory));
        }
    }

//    @Override
//    public JobExecutionResult start(TargetResName name, IDataxReader dataSource
//            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
//
//        BasicDataXRdbmsReader reader = (BasicDataXRdbmsReader) dataSource;
//        final BasicDataSourceFactory sourceFactory = (BasicDataSourceFactory) reader.getDataSourceFactory();
//        List<ReaderSource> sourceFuncs = Lists.newArrayList();
//        sourceFactory.getDbConfig().vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
//            for (ISelectedTab tab : tabs) {
//                SyncConf conf = createSyncConf(sourceFactory, jdbcUrl, dbName, (SelectedTab) tab);
//
//                AtomicReference<SourceFunction<RowData>> sourceFunc = new AtomicReference<>();
//                PostgresqlSourceFactory pgSourceFactory = new ExtendPostgresqlSourceFactory(conf, null, sourceFactory) {
//
//
//                    protected DataStream<RowData> createInput(
//                            InputFormat<RowData, InputSplit> inputFormat, String sourceName) {
//                        Preconditions.checkNotNull(sourceName);
//                        Preconditions.checkNotNull(inputFormat);
//                        DtInputFormatSourceFunction<RowData> function =
//                                new DtInputFormatSourceFunction<>(inputFormat, getTypeInformation());
//                        sourceFunc.set(function);
//                        return null;
//                    }
//                };
//                pgSourceFactory.createSource();
//
//                sourceFuncs.add(ReaderSource.createRowDataSource(dbHost + ":" + sourceFactory.port + "_" + dbName, tab, sourceFunc.get()));
//            }
//        });
//
//        try {
//            SourceChannel sourceChannel = new SourceChannel(sourceFuncs);
//            //for (ISelectedTab tab : tabs) {
//            sourceChannel.setFocusTabs(tabs, DTOStream::createRowData);
//            //}
//            return (JobExecutionResult) getConsumerHandle().consume(name, sourceChannel, dataXProcessor);
//        } catch (Exception e) {
//            throw new MQConsumeException(e.getMessage(), e);
//        }
//    }



//    private SyncConf createSyncConf(BasicDataSourceFactory sourceFactory, String jdbcUrl, String dbName, SelectedTab tab) {
//        SyncConf syncConf = new SyncConf();
//
//        JobConf jobConf = new JobConf();
//        ContentConf content = new ContentConf();
//        OperatorConf writer = new OperatorConf();
//        Map<String, Object> writerParams = Maps.newHashMap();
//        writer.setParameter(writerParams);
//
//        OperatorConf reader = new OperatorConf();
//        reader.setName("postgresqlreader");
//        Map<String, Object> params = Maps.newHashMap();
//        params.put("username", sourceFactory.getUserName());
//        params.put("password", sourceFactory.getPassword());
//
//        params.put("queryTimeOut", PostgreSQLSourceFunction.this.sourceFactory.queryTimeOut);
//        params.put("fetchSize", PostgreSQLSourceFunction.this.sourceFactory.fetchSize);
//
//        //tab.getIncrMode().set(params);
//
//        List<Map<String, String>> cols = Lists.newArrayList();
//        Map<String, String> col = null;
//        // com.dtstack.chunjun.conf.FieldConf.getField(List)
//
//        for (ISelectedTab.ColMeta cm : tab.getCols()) {
//            col = Maps.newHashMap();
//            col.put("name", cm.getName());
//            col.put("type", parseType(cm));
//            cols.add(col);
//        }
//
//        params.put(ConfigConstant.KEY_COLUMN, cols);
//        params.put("fullColumn", tab.getCols().stream().map((c) -> c.getName()).collect(Collectors.toList()));
//        Map<String, Object> conn = Maps.newHashMap();
//        conn.put("jdbcUrl", Collections.singletonList(jdbcUrl));
//        conn.put("table", Lists.newArrayList(tab.getName()));
//        //  conn.put("schema", dbName);
//        params.put("connection", Lists.newArrayList(conn));
//
//        SelectedTabPropsExtends tabExtend = tab.getIncrProps();
//        if (tabExtend == null) {
//            throw new IllegalStateException("tabExtend can not be null");
//        }
//        // tabExtend.polling.setParams(params);
//        tabExtend.setParams(params);
//
//        // polling
////        params.put("polling", true);
////        params.put("pollingInterval", 3000);
////        params.put("increColumn", "id");
////        params.put("startLocation", 0);
//
//        reader.setParameter(params);
//        content.setReader(reader);
//        content.setWriter(writer);
//        jobConf.setContent(Lists.newLinkedList(Collections.singleton(content)));
//        syncConf.setJob(jobConf);
//        return syncConf;
//    }
//
//    private String parseType(ISelectedTab.ColMeta cm) {
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
//
//    @Override
//    public String getTopic() {
//        return null;
//    }
//
//    @Override
//    public void setDeserialize(IAsyncMsgDeserialize deserialize) {
//
//    }
}
