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

package com.qlangtech.tis.plugins.incr.flink.connector.mysql;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.dtstack.chunjun.conf.ContentConf;
import com.dtstack.chunjun.conf.JobConf;
import com.dtstack.chunjun.conf.OperatorConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.mysql.sink.MysqlSinkFactory;
import com.dtstack.chunjun.constants.ConfigConstant;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;
import com.dtstack.chunjun.util.TableUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataxMySQLWriter;
import com.qlangtech.tis.plugin.datax.common.MySQLSelectedTab;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-07 20:11
 **/
public class MySQLSinkFactory extends BasicTISSinkFactory<RowData> implements IStreamTableCreator {
    //    描述：sink 端是否支持二阶段提交
//    注意：
//    如果此参数为空，默认不开启二阶段提交，即 sink 端不支持 exactly_once 语义；
//    当前只支持 exactly-once 和 at-least-once
//    必选：否
//    参数类型：String
//    示例："semantic": "exactly-once"
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String semantic;


    //    描述：一次性批量提交的记录数大小，该值可以极大减少 ChunJun 与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成 ChunJun 运行进程 OOM 情况
//    必选：否
//    参数类型：int
//    默认值：1
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public int batchSize;


    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(
                //  "tis-sink-hudi-plugin"
                MySQLSinkFactory.class
                // "tis-datax-hudi-plugin"
                // , "com.alibaba.datax.plugin.writer.hudi.HudiConfig"
        ));
    }

    @Override
    public Map<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> createSinkFunction(IDataxProcessor dataxProcessor) {
        Map<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> sinkFuncs = Maps.newHashMap();
        IDataxProcessor.TableAlias tableName = null;
        DataxMySQLWriter dataXWriter = (DataxMySQLWriter) dataxProcessor.getWriter(null);
        Map<String, IDataxProcessor.TableAlias> selectedTabs = dataxProcessor.getTabAlias();
        if (MapUtils.isEmpty(selectedTabs)) {
            throw new IllegalStateException("selectedTabs can not be empty");
        }
        IDataxReader reader = dataxProcessor.getReader(null);
        List<ISelectedTab> tabs = reader.getSelectedTabs();

        for (Map.Entry<String, IDataxProcessor.TableAlias> tabAliasEntry : selectedTabs.entrySet()) {
            tableName = tabAliasEntry.getValue();

            Objects.requireNonNull(tableName, "tableName can not be null");
            if (StringUtils.isEmpty(tableName.getFrom())) {
                throw new IllegalStateException("tableName.getFrom() can not be empty");
            }

            AtomicReference<Pair<DtOutputFormatSinkFunction<RowData>, JdbcColumnConverter>> sinkFuncRef = new AtomicReference<>();
            Pair<DtOutputFormatSinkFunction<RowData>, JdbcColumnConverter> sinkFunc = null;
            final IDataxProcessor.TableAlias tabName = tableName;
            AtomicReference<Object[]> exceptionLoader = new AtomicReference<>();
            final String targetTabName = tableName.getTo();
            MySQLDataSourceFactory dsFactory = (MySQLDataSourceFactory) dataXWriter.getDataSourceFactory();
            if (dsFactory == null) {
                throw new IllegalStateException("dsFactory can not be null");
            }
            DBConfig dbConfig = dsFactory.getDbConfig();
            dbConfig.vistDbURL(false, (dbName, jdbcUrl) -> {
                try {
                    Optional<ISelectedTab> selectedTab = tabs.stream()
                            .filter((tab) -> StringUtils.equals(tabName.getFrom(), tab.getName())).findFirst();
                    if (!selectedTab.isPresent()) {
                        throw new IllegalStateException("target table:" + tabName.getFrom()
                                + " can not find matched table in:["
                                + tabs.stream().map((t) -> t.getName()).collect(Collectors.joining(",")) + "]");
                    }
                    /**
                     * 需要先初始化表MySQL目标库中的表
                     */
                    dataXWriter.initWriterTable(targetTabName, Collections.singletonList(jdbcUrl));

                    sinkFuncRef.set(createSinkFunction(dbName, targetTabName, (MySQLSelectedTab) selectedTab.get(), jdbcUrl, dsFactory, dataXWriter));

                } catch (Throwable e) {
                    exceptionLoader.set(new Object[]{jdbcUrl, e});
                }
            });
            if (exceptionLoader.get() != null) {
                Object[] error = exceptionLoader.get();
                throw new RuntimeException((String) error[0], (Throwable) error[1]);
            }
            Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
            sinkFunc = sinkFuncRef.get();

            sinkFuncs.put(tableName, new ChunjunRowDataSinkFunc(
                    tableName, sinkFunc.getRight(), this.getStreamTableMeta(tableName.getFrom()), sinkFunc.getLeft()));
        }

        if (sinkFuncs.size() < 1) {
            throw new IllegalStateException("size of sinkFuncs can not be small than 1");
        }
        return sinkFuncs;
    }


    private Pair<DtOutputFormatSinkFunction<RowData>, JdbcColumnConverter> createSinkFunction(
            String dbName, final String targetTabName, MySQLSelectedTab tab, String jdbcUrl
            , MySQLDataSourceFactory dsFactory, DataxMySQLWriter dataXWriter) {
        SyncConf syncConf = new SyncConf();

        JobConf jobConf = new JobConf();
        ContentConf content = new ContentConf();
        OperatorConf writer = new OperatorConf();
        writer.setName("mysqlwriter");
        Map<String, Object> params = Maps.newHashMap();
        params.put("username", dsFactory.getUserName());
        params.put("password", dsFactory.getPassword());

        tab.getIncrMode().set(params);

        List<Map<String, String>> cols = Lists.newArrayList();
        Map<String, String> col = null;
        // com.dtstack.chunjun.conf.FieldConf.getField(List)

        for (ISelectedTab.ColMeta cm : tab.getCols()) {
            col = Maps.newHashMap();
            col.put("name", cm.getName());
            col.put("type", parseType(cm));
            cols.add(col);
        }

        params.put(ConfigConstant.KEY_COLUMN, cols);
        params.put("fullColumn", tab.getCols().stream().map((c) -> c.getName()).collect(Collectors.toList()));
        Map<String, Object> conn = Maps.newHashMap();
        conn.put("jdbcUrl", jdbcUrl);
        conn.put("table", Lists.newArrayList(targetTabName));
        params.put("connection", Lists.newArrayList(conn));
        writer.setParameter(params);
        content.setWriter(writer);
        jobConf.setContent(Lists.newLinkedList(Collections.singleton(content)));
        syncConf.setJob(jobConf);
        AtomicReference<Pair<DtOutputFormatSinkFunction<RowData>, JdbcColumnConverter>> ref = new AtomicReference<>();
        MysqlSinkFactory sinkFactory = new MysqlSinkFactory(syncConf) {
            @Override
            protected JdbcOutputFormatBuilder getBuilder() {
                return new JdbcOutputFormatBuilder(new TISMysqlOutputFormat(dataXWriter.dataXName));
            }
            protected DataStreamSink<RowData> createOutput(
                    DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
                JdbcOutputFormat routputFormat = (JdbcOutputFormat) outputFormat;

                try (Connection conn = dsFactory.getConnection(jdbcUrl)) {
                    routputFormat.dbConn = conn;
                    routputFormat.initColumnList();
                } catch (SQLException e) {
                    throw new RuntimeException("jdbcUrl:" + jdbcUrl, e);
                }


                // JdbcRowConverter rowConverter = (JdbcRowConverter) routputFormat.getRowConverter();

                RowType rowType =
                        TableUtil.createRowType(
                                routputFormat.columnNameList, routputFormat.columnTypeList, jdbcDialect.getRawTypeConverter());
                JdbcColumnConverter rowConverter = (JdbcColumnConverter) jdbcDialect.getColumnConverter(rowType, jdbcConf);

                //  return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
                DtOutputFormatSinkFunction<RowData> sinkFunction =
                        new DtOutputFormatSinkFunction<>(outputFormat);
                ref.set(Pair.of(sinkFunction, rowConverter));
                return null;
            }
        };
        sinkFactory.createSink(null);

        return ref.get();

    }

    private transient Map<String, MySQLSelectedTab> selTabs;

    /**
     * ==========================================================
     * impl: IStreamTableCreator
     * ===========================================================
     */
    @Override
    public IStreamTableMeta getStreamTableMeta(String tableName) {

        if (this.selTabs == null) {
            DataxProcessor dataXProcessor = DataxProcessor.load(null, this.dataXName);
            IDataxReader reader = dataXProcessor.getReader(null);
            this.selTabs
                    = reader.getSelectedTabs().stream()
                    .map((tab) -> (MySQLSelectedTab) tab).collect(Collectors.toMap((tab) -> tab.getName(), (tab) -> tab));
        }

        return new IStreamTableMeta() {
            @Override
            public List<HdfsColMeta> getColsMeta() {
                MySQLSelectedTab tab = Objects.requireNonNull(selTabs.get(tableName), "tableName:" + tableName + " relevant tab can not be null");
                return tab.getCols().stream().map((c) -> {
                    return new HdfsColMeta(c.getName(), c.isNullable(), c.isPk(), c.getType());
                }).collect(Collectors.toList());
                // return tabMeta.getRight().colMetas;
            }
        };
    }

    @Override
    public String getFlinkStreamGenerateTemplateFileName() {
        return "flink_source_handle_rowdata_scala.vm";
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return mergeData;
    }

    /**
     * ==========================================================
     * End impl: IStreamTableCreator
     * ===========================================================
     */

    private String parseType(ISelectedTab.ColMeta cm) {
        return cm.getType().accept(new DataType.TypeVisitor<String>() {
            @Override
            public String bigInt(DataType type) {
                return "BIGINT";
            }

            @Override
            public String doubleType(DataType type) {
                return "DOUBLE";
            }

            @Override
            public String dateType(DataType type) {
                return "DATE";
            }

            @Override
            public String timestampType(DataType type) {
                return "TIMESTAMP";
            }

            @Override
            public String bitType(DataType type) {
                return "BIT";
            }

            @Override
            public String blobType(DataType type) {
                // TINYBLOB、BLOB、MEDIUMBLOB、LONGBLOB
                switch (type.type) {
                    case Types.BLOB:
                        return "BLOB";
                    case Types.BINARY:
                    case Types.LONGVARBINARY:
                        return "BINARY";
                    case Types.VARBINARY:
                        return "VARBINARY";
                    default:
                        throw new IllegalStateException("illegal type:" + type.type);
                }
            }

            @Override
            public String varcharType(DataType type) {
                return "VARCHAR";
            }

            @Override
            public String intType(DataType type) {
                return "INT";
            }

            @Override
            public String floatType(DataType type) {
                return "FLOAT";
            }

            @Override
            public String decimalType(DataType type) {
                return "DECIMAL";
            }

            @Override
            public String timeType(DataType type) {
                return "TIME";
            }

            @Override
            public String tinyIntType(DataType dataType) {
                return "TINYINT";
            }

            @Override
            public String smallIntType(DataType dataType) {
                return "SMALLINT";
            }
        });
    }

    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Chunjun-MySQL-Sink";

    @TISExtension
    public static class DefaultDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected IDataXPluginMeta.EndType getTargetType() {
            return IDataXPluginMeta.EndType.MySQL;
        }
    }

}
