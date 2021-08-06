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
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriter
 **/
public class DataxMySQLWriter extends BasicDataXRdbmsWriter {
    private static final String DATAX_NAME = "MySQL";

//    @FormField(identity = false, ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String dbName;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;

//    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {})
//    public String preSql;
//
//    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {})
//    public String postSql;
//
//    @FormField(ordinal = 4, type = FormFieldType.TEXTAREA, validate = {})
//    public String session;
//
//    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
//    public Integer batchSize;

//    @FormField(ordinal = 6, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataxMySQLReader.class, "mysql-writer-tpl.json");
    }


    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        MySQLDataSourceFactory dsFactory = (MySQLDataSourceFactory) this.getDataSourceFactory();
        IDataxProcessor.TableMap tm = tableMap.get();
        if (CollectionUtils.isEmpty(tm.getSourceCols())) {
            throw new IllegalStateException("tablemap " + tm + " source cols can not be null");
        }
        TISTable table = new TISTable();
        table.setTableName(tm.getTo());
        DataDumpers dataDumpers = dsFactory.getDataDumpers(table);
        if (dataDumpers.splitCount > 1) {
            throw new IllegalStateException("dbSplit can not max than 1");
        }
        MySQLWriterContext context = new MySQLWriterContext();
        if (dataDumpers.dumpers.hasNext()) {
            IDataSourceDumper next = dataDumpers.dumpers.next();
            context.jdbcUrl = next.getDbHost();
            context.password = dsFactory.password;
            context.username = dsFactory.userName;
            context.tabName = table.getTableName();
            context.cols = IDataxProcessor.TabCols.create(tm);
            context.dbName = this.dbName;
            context.writeMode = this.writeMode;
            context.preSql = this.preSql;
            context.postSql = this.postSql;
            context.session = session;
            context.batchSize = batchSize;
            return context;
        }

        throw new RuntimeException("dbName:" + dbName + " relevant DS is empty");
    }

    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }

        DataxReader threadBingDataXReader = DataxReader.getThreadBingDataXReader();
        if (threadBingDataXReader instanceof DataxMySQLReader) {
            DataxMySQLReader mySQLReader = (DataxMySQLReader) threadBingDataXReader;
            MySQLDataSourceFactory dsFactory = mySQLReader.getDataSourceFactory();

            dsFactory.


        } else {

        }

        StringBuffer script = new StringBuffer();
        script.append("CREATE TABLE ").append(tableMapper.getTo()).append("\n");
        script.append("(\n");
        // ISelectedTab.ColMeta pk = null;
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
                    .append(convert2MySQLType(col.getType())).append(",").append("\n");
        }
        // script.append("    `__cc_ck_sign` Int8 DEFAULT 1").append("\n");
        script.append(")\n");
        script.append(" ENGINE = CollapsingMergeTree(__cc_ck_sign)").append("\n");
        // Objects.requireNonNull(pk, "pk can not be null");


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

    private String convert2MySQLType(ISelectedTab.DataXReaderColType dataxType) {
        switch (dataxType) {
            case Long:
                return "bigint";
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


    public static class MySQLWriterContext extends RdbmsDataxContext implements IDataxContext {

        private String dbName;
        private String writeMode;
        private String preSql;
        private String postSql;
        private String session;
        private Integer batchSize;

        public String getDbName() {
            return dbName;
        }

        public String getWriteMode() {
            return writeMode;
        }

        public String getPreSql() {
            return preSql;
        }

        public String getPostSql() {
            return postSql;
        }

        public String getSession() {
            return session;
        }

        public boolean isContainPreSql() {
            return StringUtils.isNotBlank(preSql);
        }

        public boolean isContainPostSql() {
            return StringUtils.isNotBlank(postSql);
        }

        public boolean isContainSession() {
            return StringUtils.isNotBlank(session);
        }

        public Integer getBatchSize() {
            return batchSize;
        }
    }

//    private DataSourceFactory getDataSourceFactory() {
//        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
//        return dsStore.getPlugin();
//    }

    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
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
