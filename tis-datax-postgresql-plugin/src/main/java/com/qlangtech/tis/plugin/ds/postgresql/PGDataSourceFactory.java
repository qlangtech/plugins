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

package com.qlangtech.tis.plugin.ds.postgresql;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.DataXPostgresqlReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.Collections;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-23 19:03
 **/
public class PGDataSourceFactory extends BasicDataSourceFactory {
    // public static final String DS_TYPE_PG = "PG";


//    // 数据库名称
//    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public String name;
//
//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String jdbcURL;
//
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public String userName;
//
//    @FormField(ordinal = 2, type = FormFieldType.PASSWORD, validate = {})
//    public String password;

//    @Override
//    public DataDumpers getDataDumpers(TISTable table) {
//        return DataDumpers.create(table);
//    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {

        String jdbcUrl = "jdbc:postgresql://" + ip + ":" + this.port + "/" + dbName;
        boolean hasParam = false;
        if (StringUtils.isNotEmpty(this.encode)) {
            hasParam = true;
            jdbcUrl = jdbcUrl + "?charSet=" + this.encode;
        }
        if (StringUtils.isNotEmpty(this.extraParams)) {
            jdbcUrl = jdbcUrl + (hasParam ? "&" : "?") + this.extraParams;
        }
        return jdbcUrl;
    }

    @Override
    protected void refectTableInDB(List<String> tabs, Connection conn) throws SQLException {
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery(
                    "SELECT table_name FROM information_schema.tables  WHERE table_schema = 'public' and table_catalog='" + this.dbName + "'");

//        DatabaseMetaData metaData = conn.getMetaData();
//        String[] types = {"TABLE"};
//        ResultSet tablesResult = metaData.getTables(conn.getCatalog(), "public", "%", types);
            while (result.next()) {
                tabs.add(result.getString(1));
            }
        } finally {
            this.closeResultSet(result);
            try {
                statement.close();
            } catch (Throwable e) { }
        }
    }

    @Override
    protected Connection getConnection(String jdbcUrl) throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(this.userName), StringUtils.trimToNull(password));
    }

//    @Override
//    public DataDumpers getDataDumpers(TISTable table) {
//        Iterator<IDataSourceDumper> dumpers = null;
//        return new DataDumpers(1, dumpers);
//    }

    @TISExtension
    public static class DefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return DataXPostgresqlReader.PG_NAME;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Collections.emptyList();
        }

//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//
//            ParseDescribable<DataSourceFactory> pgDataSource = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
//
//            try {
//                List<String> tables = pgDataSource.instance.getTablesInDB();
//                msgHandler.addActionMessage(context, "find " + tables.size() + " table in db");
//            } catch (Exception e) {
//                msgHandler.addErrorMessage(context, e.getMessage());
//                return false;
//            }
//
//            return true;
//        }
    }

}
