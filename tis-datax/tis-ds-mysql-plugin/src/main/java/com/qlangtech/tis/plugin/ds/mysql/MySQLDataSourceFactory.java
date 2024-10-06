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

package com.qlangtech.tis.plugin.ds.mysql;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataxMySQLReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
 
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.FacadeDataSource;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.IFacadeDataSource;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Driver;
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
import java.util.stream.Collectors;

/**
 * 针对MySQL作为数据源实现
 *
 * @author: baisui 百岁
 * @create: 2020-11-24 10:55
 **/
public abstract class MySQLDataSourceFactory extends BasicDataSourceFactory implements IFacadeDataSource {

    protected static final String DS_TYPE_MYSQL_V5 = DS_TYPE_MYSQL + "-V5";
    protected static final String JDBC_PARAM_AUTO_RECONNECT = "autoReconnect=true";
    /**
     * 分表策略
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public SplitTableStrategy splitTableStrategy;

    //https://blog.csdn.net/Shadow_Light/article/details/100749537
    /**
     * 传输时数据压缩
     */
    @FormField(ordinal = 14, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean useCompression;


    @Override
    protected TableInDB createTableInDB() {
        return Objects.requireNonNull(this.splitTableStrategy, "MySQL DataSourceFactory:" + this.identityValue() + " "
                + "relevant prop splitTableStrategy can not be null").createTableInDB(this);
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


    @Override
    public final Optional<String> getEscapeChar() {
        return Optional.of("`");
    }

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


    @Override
    public FacadeDataSource createFacadeDataSource() {

        final DBConfig dbConfig = this.getDbConfig();
        List<String> jdbcUrls = this.getJdbcUrls();// Lists.newArrayList();

        if (jdbcUrls.size() > 1) {
            throw new IllegalStateException("datasource count can't big than 1");
        }
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(Driver.class.getName());
        ds.setUrl(jdbcUrls.stream().findFirst().get());
        ds.setUsername(this.userName);
        ds.setPassword(this.password);
        ds.setValidationQuery("select 1");
        return new FacadeDataSource(dbConfig, ds);
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        try {
            StringBuffer jdbcUrl = new StringBuffer("jdbc:mysql://" + ip + ":" + this.port + "/" + dbName +
                    "?" + JDBC_PARAM_AUTO_RECONNECT + "&useUnicode=yes&useCursorFetch=true&useSSL=false&serverTimezone=" + URLEncoder.encode(DEFAULT_SERVER_TIME_ZONE.getId(), TisUTF8.getName()));
            if (this.useCompression != null) {
                jdbcUrl.append("&useCompression=").append(this.useCompression);
            }
            if (StringUtils.isNotEmpty(this.encode)) {
                jdbcUrl.append("&characterEncoding=").append(this.encode);
            }
            if (StringUtils.isNotEmpty(this.extraParams)) {
                jdbcUrl.append("&" + this.extraParams);
            }
            return jdbcUrl.toString();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
                int idx;
                final String jdbcUrl = jdbcUrls.get(idx = index.getAndIncrement());
                if (jdbcUrl == null) {
                    throw new IllegalStateException("jdbcUrl can not be empty,jdbcUrls.size:" + length
                            + ",applyIdx:" + idx + ",jdbcUrls:"
                            + jdbcUrls.stream().map((url) -> "[" + url + "]").collect(Collectors.joining(",")));
                }
                return new MySqlDataSourceDumper(jdbcUrl, table);
            }
        };

        return new DataDumpers(length, dsIt);
    }

    @Override
    protected String getNodeDesc() {
        return Objects.requireNonNull(
                this.splitTableStrategy, "splitTableStrategy can not be null").getNodeDesc();
    }

    public String getUserName() {
        return this.userName;
    }

    public String getPassword() {
        return this.password;
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

    public static abstract class DefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {
        @Override
        public boolean supportFacade() {
            return true;
        }

        @Override
        public Optional<String> getDefaultDataXReaderDescName() {
            return Optional.of(DataxMySQLReader.DATAX_NAME);
        }

        @Override
        public EndType getEndType() {
            return EndType.MySQL;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Lists.newArrayList(DS_TYPE_MYSQL_V5);
        }


        @Override
        protected boolean validateConnection(JDBCConnection c, BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {


            String mysqlVer = null;
            try (Statement statement = c.createStatement()) {
                try (ResultSet result = statement.executeQuery("SELECT version()")) {
                    if (result.next()) {
                        mysqlVer = result.getString(1);
                        if (!validateMySQLVer(mysqlVer)) {
                            throw TisException.create("实际MySQL版本:'" + mysqlVer + "'与插件:'" + getDataSourceName() + "'支持的版本不一致");
                        }
                    } else {
                        throw new SQLException("have not find any MySQL version info ");
                    }
                }
            } catch (TisException e) {
                throw e;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        protected abstract boolean validateMySQLVer(String mysqlVer);
    }

}
