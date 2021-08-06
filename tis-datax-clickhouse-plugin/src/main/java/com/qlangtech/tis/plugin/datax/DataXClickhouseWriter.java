/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * https://github.com/alibaba/DataX/blob/master/clickhousewriter/src/main/resources/plugin_job_template.json
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-16 21:48
 * @see com.alibaba.datax.plugin.writer.clickhousewriter.TISClickhouseWriter
 **/
public class DataXClickhouseWriter extends BasicDataXRdbmsWriter<ClickHouseDataSourceFactory>
        implements KeyedPluginStore.IPluginKeyAware {

    public static final String DATAX_NAME = "ClickHouse";

//    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String dbName;
//    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
//    public String preSql;
//    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
//    public String postSql;
//    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
//    public Integer batchSize;

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer batchByteSize;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;




//    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;

    public String dataXName;

    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }

        // List<ColumnMetaData> tableMetadata = this.getDataSourceFactory().getTableMetadata(tableMapper.getTo());
        //Set<String> pks = tableMetadata.stream().filter((t) -> t.isPk()).map((t) -> t.getName()).collect(Collectors.toSet());

        StringBuffer script = new StringBuffer();
        script.append("CREATE TABLE ").append(tableMapper.getTo()).append("\n");
        script.append("(\n");
        ISelectedTab.ColMeta pk = null;
        int maxColNameLength = 0;
        for (ISelectedTab.ColMeta col : tableMapper.getSourceCols()) {
            int m = StringUtils.length(col.getName());
            if (m > maxColNameLength) {
                maxColNameLength = m;
            }
        }
        maxColNameLength += 4;
        for (ISelectedTab.ColMeta col : tableMapper.getSourceCols()) {
            if (pk == null && col.isPk()) {
                pk = col;
            }
            script.append("    `").append(String.format("%-" + (maxColNameLength) + "s", col.getName() + "`"))
                    .append(convert2ClickhouseType(col.getType())).append(",").append("\n");
        }
        script.append("    `__cc_ck_sign` Int8 DEFAULT 1").append("\n");
        script.append(")\n");
        script.append(" ENGINE = CollapsingMergeTree(__cc_ck_sign)").append("\n");
        // Objects.requireNonNull(pk, "pk can not be null");
        if (pk != null) {
            script.append(" ORDER BY `").append(pk.getName()).append("`\n");
        }
        script.append(" SETTINGS index_granularity = 8192");
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
        return script;
    }

    private String convert2ClickhouseType(ISelectedTab.DataXReaderColType dataxType) {
        switch (dataxType) {
            case Long:
                return "Int64";
            case INT:
                return "Int32";
            case Double:
                return "Float64";
            case Date:
                return "Date";
            case STRING:
            case Boolean:
            case Bytes:
            default:
                return "String";
        }

    }

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public ClickHouseDataSourceFactory getDataSourceFactory() {
        return super.getDataSourceFactory();
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
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
        context.setCols(IDataxProcessor.TabCols.create(tableMap.get()));
        return context;
    }

//    @Override
//    public String getTemplate() {
//        return template;
//    }


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
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
