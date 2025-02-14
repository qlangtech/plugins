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

import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * https://github.com/alibaba/DataX/blob/master/clickhousewriter/src/main/resources/plugin_job_template.json
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-16 21:48
 * @see com.alibaba.datax.plugin.writer.clickhousewriter.TISClickhouseWriter
 **/
@Public
public class DataXClickhouseWriter extends BasicDataXRdbmsWriter<ClickHouseDataSourceFactory>
        implements KeyedPluginStore.IPluginKeyAware {

    public static final String DATAX_NAME = "ClickHouse";

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer batchByteSize;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;


//    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;

    // public String dataXName;

//    @Override
//    public void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
//        InitWriterTable.process(this.dataXName, (BasicDataXRdbmsWriter) this, targetTabName, jdbcUrls);
//    }

//    @Override
//    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(SourceColMetaGetter sourceColMetaGetter, IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
//        if (!this.autoCreateTable) {
//            return null;
//        }
//        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder<ColWrapper>(tableMapper, this.getDataSourceFactory(), transformers) {
//            @Override
//            protected void appendExtraColDef(List<String> pks) {
//                script.append("   ," + wrapWithEscape(ClickHouseCommon.KEY_CLICKHOUSE_CK) + " Int8 DEFAULT 1").append("\n");
//            }
//
//            @Override
//            protected ColWrapper createColWrapper(IColMetaGetter c) {
//                return new ColWrapper(c, this.pks) {
//                    @Override
//                    public String getMapperType() {
//                        return convertType(this.getType());
//                    }
//
//                    @Override
//                    protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
//                        autoCreateTable.addStandardColComment(sourceColMetaGetter,tableMapper,this,ddlScript);
//                    }
//                };
//            }
//
//            @Override
//            protected void appendTabMeta(List<String> pk) {
//                script.append(" ENGINE = CollapsingMergeTree(" + ClickHouseCommon.KEY_CLICKHOUSE_CK + ")").append("\n");
//                if (CollectionUtils.isNotEmpty(pk)) {
//                    script.append(" ORDER BY (").append(pk.stream().map((p) -> this.wrapWithEscape(p)).collect(Collectors.joining(","))).append(")\n");
//                }
//                script.append(" SETTINGS index_granularity = 8192");
//            }
//
//            private String convertType(DataType type) {
//
//                switch (Objects.requireNonNull(type, "type can not be null")
//                        .getJdbcType()) {
//                    case INTEGER:
//                    case TINYINT:
//                    case SMALLINT:
//                        return "Int32";
//                    case BIGINT:
//                        return "Int64";
//                    case FLOAT:
//                        return "Float32";
//                    case DOUBLE:
//                    case DECIMAL:
//                        return "Float64";
//                    case DATE:
//                        return "Date";
//                    case TIME:
//                    case TIMESTAMP:
//                        return "DateTime";
//                    case BIT:
//                    case BOOLEAN:
//                        return "UInt8";
//                    case BLOB:
//                    case BINARY:
//                    case LONGVARBINARY:
//                    case VARBINARY:
//                    default:
//                        return "String";
//                }
//            }
//        };
//        return createTableSqlBuilder.build();
        // List<ColumnMetaData> tableMetadata = this.getDataSourceFactory().getTableMetadata(tableMapper.getTo());
        //Set<String> pks = tableMetadata.stream().filter((t) -> t.isPk()).map((t) -> t.getName()).collect(Collectors.toSet());

//        StringBuffer script = new StringBuffer();
//        script.append("CREATE TABLE ").append(tableMapper.getTo()).append("\n");
//        script.append("(\n");
//        CMeta pk = null;
//        int maxColNameLength = 0;
//        for (CMeta col : tableMapper.getSourceCols()) {
//            int m = StringUtils.length(col.getName());
//            if (m > maxColNameLength) {
//                maxColNameLength = m;
//            }
//        }
//        maxColNameLength += 4;
//        for (CMeta col : tableMapper.getSourceCols()) {
//            if (pk == null && col.isPk()) {
//                pk = col;
//            }
//            script.append("    `").append(String.format("%-" + (maxColNameLength) + "s", col.getName() + "`"))
//                    .append(convert2ClickhouseType(col.getType())).append(",").append("\n");
//        }
//        script.append("    `__cc_ck_sign` Int8 DEFAULT 1").append("\n");
//        script.append(")\n");
//        script.append(" ENGINE = CollapsingMergeTree(__cc_ck_sign)").append("\n");
//        // Objects.requireNonNull(pk, "pk can not be null");
//        if (pk != null) {
//            script.append(" ORDER BY `").append(pk.getName()).append("`\n");
//        }
//        script.append(" SETTINGS index_granularity = 8192");
//        CREATE TABLE tis.customer_order_relation
//                (
//                        `customerregister_id` String,
//                        `waitingorder_id` String,
//                        `worker_id` String,
//                        `kind` Int8,
//                        `create_time` Int64,
//                        `last_ver` Int8,
//                        `__cc_ck_sign` Int8 DEFAULT 1
//                )
//        ENGINE = CollapsingMergeTree(__cc_ck_sign)
//        ORDER BY customerregister_id
//        SETTINGS index_granularity = 8192
        //     return script;
   // }


//    @Override
//    public void setKey(KeyedPluginStore.Key key) {
//        this.dataXName = key.keyVal.getVal();
//    }



    @Override
    public ClickHouseDataSourceFactory getDataSourceFactory() {
        return super.getDataSourceFactory();
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("tableMap shall be present");
        }
        IDataxProcessor.TableMap tmapper = tableMap.get();
        ClickHouseDataSourceFactory ds = this.getDataSourceFactory();

        ClickHouseWriterContext context = new ClickHouseWriterContext();
        context.setDataXName(this.dataXName);
        context.setBatchByteSize(this.batchByteSize);
        context.setBatchSize(this.batchSize);
        for (String jdbcUrl : ds.getJdbcUrls()) {
            context.setJdbcUrl(jdbcUrl);
            break;
        }
        if (StringUtils.isEmpty(context.getJdbcUrl())) {
            throw new IllegalStateException("jdbcUrl can not be null");
        }
        context.setUsername(ds.getUserName());
        context.setPassword(ds.password);
        context.setTable(tmapper.getTo());
        context.setWriteMode(this.writeMode);
        //  PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");
        //  Map<String, String> params = new HashMap<>();
        //params.put("table", tmapper.getTo());
//        PropertyPlaceholderHelper.PlaceholderResolver resolver = (key) -> {
//            return params.get(key);
//        };
        if (StringUtils.isNotEmpty(this.preSql)) {
            // context.setPreSql(helper.replacePlaceholders(this.preSql, resolver));
            context.setPreSql(this.preSql);
        }

        if (StringUtils.isNotEmpty(this.postSql)) {
            // context.setPostSql(helper.replacePlaceholders(this.postSql, resolver));
            context.setPostSql(this.postSql);
        }
        context.setCols(IDataxProcessor.TabCols.create(ds, tableMap.get(), transformerRules));
        return context;
    }


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(
                DataXClickhouseWriter.class, "DataXClickhouseWriter-tpl.json");
    }

//    public ClickHouseDataSourceFactory getDataSourceFactory() {
//        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
//        return dsStore.getDataSource();
//    }


    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        protected int getMaxBatchSize() {
            return 3072;
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Clickhouse;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
