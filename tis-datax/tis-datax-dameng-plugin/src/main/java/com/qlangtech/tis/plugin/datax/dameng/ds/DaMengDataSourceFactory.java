package com.qlangtech.tis.plugin.datax.dameng.ds;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.dameng.reader.DataXDaMengReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import dm.jdbc.driver.DmdbType;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * https://eco.dameng.com/document/dm/zh-cn/pm/jdbc-rogramming-guide.html
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/14
 */
public class DaMengDataSourceFactory extends BasicDataSourceFactory implements DataSourceFactory.ISchemaSupported {
    /**
     * 分表策略
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public SplitTableStrategy splitTableStrategy;

    //https://blog.csdn.net/Shadow_Light/article/details/100749537

    @Override
    public TableInDB createTableInDB() {
        return Objects.requireNonNull(this.splitTableStrategy, "DaMeng DataSourceFactory:" + this.identityValue() + " "
                + "relevant prop splitTableStrategy can not be null").createTableInDB(this);
    }

    @Override
    protected String getNodeDesc() {
        return Objects.requireNonNull(splitTableStrategy, "splitTableStrategy can not be null").getNodeDesc();
    }

    @Override
    public void fillTableInDB(TableInDB tabs) {
        super.fillTableInDB(tabs);
    }

    @Override
    protected String getRefectTablesSql() {
        return "SELECT owner ||'.'|| table_name FROM all_tables WHERE REGEXP_INSTR(table_name,'[\\.$#]+') < 1";
    }

    @Override
    public List<String> getAllPhysicsTabs(DataXJobSubmit.TableDataXEntity tabEntity) {
        // return super.getAllPhysicsTabs(tabEntity);
        return this.splitTableStrategy.getAllPhysicsTabs(this, tabEntity);
    }

    @Override
    protected EntityName logicTable2PhysicsTable(String jdbcUrl, EntityName table) {
        if (table.isPhysics()) {
            return table;
        }
        // return super.logicTable2PhysicsTable(table);
        SplitTableStrategy.DBPhysicsTable physicsTable = Objects.requireNonNull(this.splitTableStrategy,
                "splitTableStrategy can not be null").getMatchedPhysicsTable(this, jdbcUrl, table);
        return physicsTable.getPhysicsTab();
    }

    private transient dm.jdbc.driver.DmDriver driver;

    @Override
    public JDBCConnection getConnection(String jdbcUrl, boolean verify) throws SQLException {
        if (driver == null) {
            driver = new dm.jdbc.driver.DmDriver();
        }
        java.util.Properties info = new java.util.Properties();

        if (this.userName != null) {
            info.put("user", this.userName);
        }
        if (password != null) {
            info.put("password", password);
        }
        //info.put("connectTimeout", "60000");
        Connection connect = driver.connect(jdbcUrl, info);
        connect.setAutoCommit(true);
        return new JDBCConnection(connect, jdbcUrl);
    }


    @Override
    public final Optional<String> getEscapeChar() {
        return Optional.of("\"");
    }

    /**
     * @param inSink
     * @param table
     * @param columns1
     * @param pkCols
     * @return
     * @throws SQLException
     * @see DmdbType 内部方法（d2jType）会将达梦内部的类型转成jdbc type
     */
    @Override
    public List<ColumnMetaData> wrapColsMeta(boolean inSink, EntityName table, ResultSet columns1,
                                             Set<String> pkCols) throws SQLException, TableNotFoundException {

        return this.wrapColsMeta(inSink, table, columns1, new CreateColumnMeta(pkCols, columns1) {
            @Override
            protected DataType getDataType(String colName) throws SQLException {
                DataType type = super.getDataType(colName);
                DataType fixType = type.accept(new DataType.TypeVisitor<DataType>() {
                    @Override
                    public DataType bigInt(DataType type) {
                        if (type.isUnsigned() && !pkCols.contains(colName) /**不能是主键，例如转换成doris时候 主键如果是decimal的话
                         建表的ddl会有问题*/) {
                            DataType t = DataType.create(Types.NUMERIC, type.typeName, type.getColumnSize());
                            t.setDecimalDigits(0);
                            return t;
                        }
                        return null;
                    }

                    @Override
                    public DataType tinyIntType(DataType dataType) {
                        if (dataType.isUnsigned()) {
                            // 如果为unsigned则会按照一个byte来进行处理，需要将其变成small int
                            return DataType.create(Types.SMALLINT, type.typeName, type.getColumnSize());
                        }
                        return null;
                    }

                    @Override
                    public DataType smallIntType(DataType dataType) {

                        if (dataType.isUnsigned()) {
                            // 如果为unsigned则会按照一个short来进行处理，需要将其变成small int
                            return DataType.create(Types.INTEGER, type.typeName, type.getColumnSize());
                        }

                        return null;
                    }

                    @Override
                    public DataType decimalType(DataType type) {
                        if ("number".equalsIgnoreCase(type.typeName) && type.getColumnSize() < 1) {
                            // 用户使用 number 类型没有默认值会导致下游doris 的decimal(0,0) 的问题
//                            CREATE TABLE TIS.NUMERIC_TEST(
//                                    cust_id INT NOT NULL,
//                                    amt_num NUMBER default 0
//                            );

                            DataType fix = DataType.create(type.getType(), type.typeName, 25);
                            fix.setDecimalDigits(0);
                            return fix;
                        }

                        return null;
                    }

                    @Override
                    public DataType intType(DataType type) {
                        if (type.isUnsigned()) {
                            return DataType.create(Types.BIGINT, type.typeName, type.getColumnSize());
                        }
                        return null;
                    }

                    @Override
                    public DataType doubleType(DataType type) {
                        return null;
                    }

                    @Override
                    public DataType dateType(DataType type) {
                        if ("year".equalsIgnoreCase(type.typeName)) {
                            return DataType.create(Types.INTEGER, type.typeName, type.getColumnSize());
                        }
                        return null;
                    }

                    @Override
                    public DataType timestampType(DataType type) {
                        return null;
                    }

                    @Override
                    public DataType bitType(DataType type) {
                        if (type.getColumnSize() > 1) {
                            return DataType.create(Types.BINARY, type.typeName, type.getColumnSize());
                        }
                        return null;
                    }

                    @Override
                    public DataType blobType(DataType type) {
                        return null;
                    }


                    @Override
                    public DataType varcharType(DataType type) {
                        if (type.getColumnSize() < 1) {
                            // 数据库中如果是json类型的，colSize会是0，在这里需要将它修正一下
                            DataType n = DataType.create(Types.VARCHAR, type.typeName, 2000);
                            n.setDecimalDigits(type.getDecimalDigits());
                            return n;
                        }
                        return null;
                    }
                });
                return fixType != null ? fixType : type;
            }
        });
    }


    /**
     * https://eco.dameng.com/document/dm/zh-cn/app-dev/java-jdbc.html#2.1%20%E5%87%86%E5%A4%87%20DM-JDBC%20%E9%A9%B1%E5%8A%A8%E5%8C%85 <br/>
     * <p>
     * 参数说明：
     * https://eco.dameng.com/document/dm/zh-cn/pm/jdbc-rogramming-guide.html
     *
     * @param db
     * @param ip
     * @param dbName
     * @return
     */
    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        StringBuffer jdbcUrl = new StringBuffer("jdbc:dm://" + ip + ":" + this.port + "?schema=" + dbName);
        if (StringUtils.isNotBlank(this.extraParams)) {
            jdbcUrl.append("&").append(this.extraParams);
        }
        return jdbcUrl.toString();
    }

    @Override
    public DataDumpers getDataDumpers(TISTable table) {
        if (table == null) {
            throw new IllegalArgumentException("param table can not be null");
        }
        List<String> jdbcUrls = getJdbcUrls();
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


    public String getUserName() {
        return this.userName;
    }

    public String getPassword() {
        return this.password;
    }

    @Override
    public String getDBSchema() {
        return this.dbName;
    }

    private class MySqlDataSourceDumper implements IDataSourceDumper {
        private final String jdbcUrl;
        private final TISTable table;

        private JDBCConnection connection;
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
            try {
                // validateConnection(jdbcUrl, (conn) -> {
                Statement statement = null;
                ResultSet result = null;
                try {
                    StringBuffer refactSql = parseRowCountSql();
                    statement = connection.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
                    result = statement.executeQuery(refactSql.toString());
                    result.last();
                    final int rowSize = result.getRow();
                    count[0] = rowSize;
                } finally {
                    closeResultSet(result);
                    closeResultSet(statement);
                }
                //});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
                    result.add(new ColumnMetaData((i - 1), metaData.getColumnLabel(i),
                            new DataType(JDBCTypes.parse(metaData.getColumnType(i))), false, true));
                }
                return result;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<Map<String, Object>> startDump() {
            String executeSql = table.getSelectSql();
            if (StringUtils.isEmpty(executeSql)) {
                throw new IllegalStateException("executeSql can not be null");
            }
            try {
                this.connection = getConnection(jdbcUrl, false);
                this.statement = connection.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                this.resultSet = statement.executeQuery(executeSql);
                ResultSetMetaData metaData = resultSet.getMetaData();
                this.columCount = metaData.getColumnCount();
                this.colMeta = buildColumnMetaData(metaData);


                final ResultSet result = resultSet;
                return new Iterator<Map<String, Object>>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            return result.next();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public Map<String, Object> next() {
                        Map<String, Object> row = new LinkedHashMap<>(columCount);
                        String key = null;
                        String value = null;


                        for (ColumnMetaData colMeta : colMeta) {

                            key = colMeta.getKey(); //metaData.getColumnLabel(i);
                            // 防止特殊字符造成HDFS文本文件出现错误
                            value = filter(resultSet, colMeta);
                            // 在数据来源为数据库情况下，客户端提供一行的数据对于Solr来说是一个Document
                            row.put(key, value != null ? value : "");
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
            value = resultSet.getString(colMeta.getIndex() + 1);
        } catch (Throwable e) {
            return null;
        }

        if (colMeta.getType().getJdbcType() == JDBCTypes.VARCHAR || colMeta.getType().getJdbcType() == JDBCTypes.BLOB) {
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


    private void closeResultSet(JDBCConnection rs) {
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


    @TISExtension
    public static class DefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {
        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        protected String getDataSourceName() {
            return DataXDaMengReader.DATAX_NAME;
        }

        @Override
        protected boolean validateConnection(JDBCConnection conn, BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {
            final String schema = conn.getSchema();
            DaMengDataSourceFactory damengSource = (DaMengDataSourceFactory) dsFactory;
            if (!StringUtils.equals(schema, damengSource.dbName)
                    && StringUtils.equalsIgnoreCase(schema, damengSource.dbName)) {
                // 说明大小写不一致
                msgHandler.addFieldError(context, KEY_FIELD_DB_NAME, "与数据库实际名称大小写不一致，实际值为：" + schema);
                return false;
            }
            return true;
        }

        @Override
        public Optional<String> getDefaultDataXReaderDescName() {
            return Optional.of(DataXDaMengReader.DATAX_NAME);
        }

        @Override
        public final EndType getEndType() {
            return EndType.DaMeng;
        }

    }
}
