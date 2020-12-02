package com.qlangtech.tis.plugin.ds.mysql;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Sets;
import com.qlangtech.tis.db.parser.DBConfigParser;
import com.qlangtech.tis.db.parser.domain.DBConfig;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * 针对MySQL作为数据源实现
 *
 * @author: baisui 百岁
 * @create: 2020-11-24 10:55
 **/
public class MySQLDataSourceFactory extends DataSourceFactory {
    static {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // 数据库名称
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String dbName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String userName;

    @FormField(ordinal = 2, type = FormFieldType.PASSWORD, validate = {Validator.require})
    public String password;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public int port;
    /**
     * 数据库编码
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require, Validator.identity})
    public String encode;
    /**
     * 附加参数
     */
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT)
    public String extraParams;
    /**
     * 节点描述
     */
    @FormField(ordinal = 6, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String nodeDesc;

    @Override
    public DataSource createFacadeDataSource() {
        return null;
    }

    @Override
    public Iterator<IDataSourceDumper> getDataDumpers() {
        return null;
    }


    @Override
    public List<ColumnMetaData> getTableMetadata(final String table) {
        if (StringUtils.isBlank(table)) {
            throw new IllegalArgumentException("param table can not be null");
        }
        List<ColumnMetaData> columns = new ArrayList<>();
        try {

            final DBConfig dbConfig = getDbConfig();
            dbConfig.vistDbName((config, ip, dbname) -> {
                visitConnection(config, ip, dbname, config.getUserName(), config.getPassword(), (conn) -> {
                    DatabaseMetaData metaData1 = null;
                    ResultSet primaryKeys = null;
                    ResultSet columns1 = null;
                    try {
                        metaData1 = conn.getMetaData();
                        primaryKeys = metaData1.getPrimaryKeys(null, null, table);
                        columns1 = metaData1.getColumns(null, null, table, null);
                        Set<String> pkCols = Sets.newHashSet();
                        while (primaryKeys.next()) {
                            // $NON-NLS-1$
                            String columnName = primaryKeys.getString("COLUMN_NAME");
                            pkCols.add(columnName);
                        }
                        int i = 0;
                        String colName = null;
                        while (columns1.next()) {
                            columns.add(new ColumnMetaData((i++), (colName = columns1.getString("COLUMN_NAME")), columns1.getInt("DATA_TYPE"), pkCols.contains(colName)));
                        }

                    } finally {
                        closeResultSet(columns1);
                        closeResultSet(primaryKeys);
                    }
                });
                return true;
            });
            return columns;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
                ;
            }
        }
    }

    @Override
    public List<String> getTablesInDB() throws Exception {
        final List<String> tabs = new ArrayList<>();

        final DBConfig dbConfig = getDbConfig();

        dbConfig.vistDbName((config, ip, databaseName) -> {
            visitConnection(config, ip, databaseName, config.getUserName(), config.getPassword(), (conn) -> {
                Statement statement = null;
                ResultSet resultSet = null;
                try {
                    statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.execute("show tables");
                    resultSet = statement.getResultSet();
                    while (resultSet.next()) {
                        tabs.add(resultSet.getString(1));
                    }
                } finally {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                }
            });
            return true;
        });
        return tabs;
    }

    private DBConfig getDbConfig() {
        final DBConfig dbConfig = new DBConfig();
        dbConfig.setName(this.dbName);
        dbConfig.setPassword(this.password);
        dbConfig.setUserName(this.userName);
        dbConfig.setPort(this.port);
        dbConfig.setDbEnum(DBConfigParser.parseDBEnum(dbName, this.nodeDesc));
        return dbConfig;
    }

    @Override
    public String getName() {
        if (StringUtils.isEmpty(this.dbName)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return this.dbName;
    }


    private void visitConnection(DBConfig db, String ip, String dbName, String username, String password, IConnProcessor p) throws Exception {
        if (db == null) {
            throw new IllegalStateException("param db can not be null");
        }
        if (StringUtils.isEmpty(ip)) {
            throw new IllegalArgumentException("param ip can not be null");
        }
        if (StringUtils.isEmpty(dbName)) {
            throw new IllegalArgumentException("param dbName can not be null");
        }
        if (StringUtils.isEmpty(username)) {
            throw new IllegalArgumentException("param username can not be null");
        }
        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("param password can not be null");
        }
        if (p == null) {
            throw new IllegalArgumentException("param IConnProcessor can not be null");
        }
        Connection conn = null;
        String jdbcUrl = "jdbc:mysql://" + ip + ":" + db.getPort() + "/" + dbName + "?useUnicode=yes";
        if (StringUtils.isNotEmpty(this.encode)) {
            jdbcUrl = jdbcUrl + "&characterEncoding=" + this.encode;
        }
        if (StringUtils.isNotEmpty(this.extraParams)) {
            jdbcUrl = jdbcUrl + "&" + this.extraParams;
        }
        try {
            validateConnection(jdbcUrl, db, username, password, p);
        } catch (Exception e) {
            throw new RuntimeException(jdbcUrl, e);
        }
    }

    private static void validateConnection(String jdbcUrl, DBConfig db, String username, String password, IConnProcessor p) throws Exception {
        Connection conn = null;
        try {

            conn = DriverManager.getConnection(jdbcUrl, username, password);
            p.vist(conn);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Throwable e) {
                }
            }
        }
    }

    public interface IConnProcessor {
        public void vist(Connection conn) throws SQLException;
    }


    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_MYSQL;
        }
        @Override
        public boolean supportFacade() {
            return true;
        }
        @Override
        public List<String> facadeSourceTypes() {
            return Collections.singletonList(DS_TYPE_MYSQL);
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ParseDescribable<DataSourceFactory> mysqlDS = this.newInstance(postFormVals.rawFormData);

            try {
                mysqlDS.instance.getTablesInDB();
            } catch (Exception e) {
                msgHandler.addErrorMessage(context, e.getMessage());
                return false;
            }

            return true;
        }
    }

}
