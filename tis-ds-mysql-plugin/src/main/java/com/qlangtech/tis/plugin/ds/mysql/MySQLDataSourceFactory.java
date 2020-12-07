package com.qlangtech.tis.plugin.ds.mysql;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.db.parser.DBConfigParser;
import com.qlangtech.tis.db.parser.domain.DBConfig;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    public DataDumpers getDataDumpers(TISTable table) {
        if (table == null) {
            throw new IllegalArgumentException("param table can not be null");
        }
        final DBConfig dbLinkMetaData = this.getDbConfig();
        List<String> jdbcUrls = Lists.newArrayList();
        final DataSourceRegister.DBRegister dbRegister
                = new DataSourceRegister.DBRegister(dbLinkMetaData.getName(), dbLinkMetaData) {
            @Override
            protected void createDefinition(String dbDefinitionId, String driverClassName, String jdbcUrl, String userName, String password) {
                jdbcUrls.add(jdbcUrl);
//                BasicDataSource ds = new BasicDataSource();
//                ds.setDriverClassName(driverClassName);
//                ds.setUrl(jdbcUrl);
//                ds.setUsername(userName);
//                ds.setPassword(password);
//                ds.setValidationQuery("select 1");
//                synchronized (dbNames) {
//                    dsMap.put(dbDefinitionId, ds);
//                    dbCount.incrementAndGet();
//                    dbNames.append(dbDefinitionId).append(";");
//                }
            }
        };
        dbRegister.visitAll();
        final int length = jdbcUrls.size();
        final AtomicInteger index = new AtomicInteger();
        Iterator<IDataSourceDumper> dsIt = new Iterator<IDataSourceDumper>() {
            @Override
            public boolean hasNext() {
                return index.get() < length;
            }

            @Override
            public IDataSourceDumper next() {
                final String jdbcUrl = jdbcUrls.get(index.getAndIncrement());
                return new MySqlDataSourceDumper(jdbcUrl, table);
            }
        };

        return new DataDumpers(length, dsIt);
    }

    private class MySqlDataSourceDumper implements IDataSourceDumper {
        private final String jdbcUrl;
        private final TISTable table;

        private Connection connection;
        private Statement statement;
        // private ResultSetMetaData metaData;

        private List<ColumnMetaData> colMeta;

        private ResultSet resultSet;
        int columCount;

        public MySqlDataSourceDumper(String jdbcUrl, TISTable table) {
            this.jdbcUrl = jdbcUrl;
            if (table == null || StringUtils.isEmpty(table.getTableName())) {
                throw new IllegalArgumentException("param table either instance or tableName of instance is empty");
            }
            this.table = table;
        }

        @Override
        public void closeResource() {
            columCount = -1;
            closeResultSet(resultSet);
            closeResultSet(statement);
            closeResultSet(connection);
        }

        @Override
        public int getRowSize() {
            int[] count = new int[1];
            validateConnection(jdbcUrl, null, userName, password, (connection) -> {
                Statement statement = null;
                ResultSet result = null;
                try {
                    StringBuffer refactSql = parseRowCountSql();
                    statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    result = statement.executeQuery(refactSql.toString());
                    result.last();
                    final int rowSize = result.getRow();
                    count[0] = rowSize;
                } finally {
                    closeResultSet(result);
                    closeResultSet(statement);
                }
            });
            return count[0];
        }

        private StringBuffer parseRowCountSql() {

            StringBuffer refactSql = new StringBuffer("SELECT 1 FROM ");

            refactSql.append(this.table.getTableName());
            // FIXME where 先缺省以后加上
            return refactSql;
        }


        @Override
        public List<ColumnMetaData> getMetaData() {
            if (this.colMeta == null) {
                throw new IllegalStateException("colMeta can not be null");
            }
            return this.colMeta;


        }

        private List<ColumnMetaData> buildColumnMetaData(ResultSetMetaData metaData) {
            if (columCount < 1 || metaData == null) {
                throw new IllegalStateException("shall execute startDump first");
            }
            List<ColumnMetaData> result = new ArrayList<>();
            try {
                for (int i = 1; i <= columCount; i++) {
                    result.add(new ColumnMetaData((i - 1), metaData.getColumnLabel(i), metaData.getColumnType(i), false));
                }
                return result;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<Map<String, String>> startDump() {
            String executeSql = table.getSelectSql();
            if (StringUtils.isEmpty(executeSql)) {
                throw new IllegalStateException("executeSql can not be null");
            }
            try {
                this.connection = getConnection(jdbcUrl, userName, password);
                this.statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                this.resultSet = statement.executeQuery(executeSql);
                ResultSetMetaData metaData = resultSet.getMetaData();
                this.columCount = metaData.getColumnCount();
                this.colMeta = buildColumnMetaData(metaData);


                final ResultSet result = resultSet;
                return new Iterator<Map<String, String>>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            return result.next();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public Map<String, String> next() {
                        Map<String, String> row = new LinkedHashMap<>(columCount);
                        String key = null;
                        String value = null;


                        for (ColumnMetaData colMeta : colMeta) {

                            key = colMeta.getKey(); //metaData.getColumnLabel(i);
                            // 防止特殊字符造成HDFS文本文件出现错误
                            value = filter(resultSet, colMeta);
                            // 在数据来源为数据库情况下，客户端提供一行的数据对于Solr来说是一个Document
                            row.put(key, value != null ? value : "");
                        }

                        for (int i = 1; i <= columCount; i++) {

                        }
                        return row;
                    }
                };
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getDbHost() {
            return this.jdbcUrl;
        }
    }

    public static String filter(ResultSet resultSet, ColumnMetaData colMeta) {
        String value = null;
        try {
            value = resultSet.getString(colMeta.getIndex());
        } catch (Throwable e) {
            return null;
        }

        if (colMeta.getType() == Types.VARCHAR || colMeta.getType() == Types.BLOB) {
            return filter(value);
        } else {
            return value;
        }
    }

    public static String filter(String input) {
        if (input == null) {
            return input;
        }
        StringBuffer filtered = new StringBuffer(input.length());
        char c;
        for (int i = 0; i <= input.length() - 1; i++) {
            c = input.charAt(i);
            switch (c) {
                case '\t':
                    break;
                case '\r':
                    break;
                case '\n':
                    break;
                default:
                    filtered.append(c);
            }
        }
        return (filtered.toString());
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


    private void closeResultSet(Connection rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
                ;
            }
        }
    }

    private void closeResultSet(Statement rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
                ;
            }
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

    private static void validateConnection(String jdbcUrl, DBConfig db, String username, String password, IConnProcessor p) {
        Connection conn = null;
        try {
            conn = getConnection(jdbcUrl, username, password);
            p.vist(conn);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Throwable e) {
                }
            }
        }
    }

    private static Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
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
