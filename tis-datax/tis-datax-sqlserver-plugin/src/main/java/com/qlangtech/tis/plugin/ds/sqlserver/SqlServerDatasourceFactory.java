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

package com.qlangtech.tis.plugin.ds.sqlserver;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataXSqlserverReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.timezone.TISTimeZone;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-07 09:47
 **/
@Public
public abstract class SqlServerDatasourceFactory extends BasicDataSourceFactory implements BasicDataSourceFactory.ISchemaSupported {
    private static final String DS_TYPE_SQL_SERVER = "SqlServer";

    public static final String SQL_SERVER_VERSION_2019 = "2019";
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String tabSchema;
    /**
     * 分表策略
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public SplitTableStrategy splitTableStrategy;

//    @FormField(ordinal = 13, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String timeZone;

    @FormField(ordinal = 13, validate = {Validator.require})
    public TISTimeZone timeZone;

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        String jdbcUrl = "jdbc:sqlserver://" + ip + ":" + this.port + ";databaseName=" + dbName + ";encrypt=false";
        if (StringUtils.isNotEmpty(this.extraParams)) {
            jdbcUrl = jdbcUrl + ";" + this.extraParams;
        }
        return jdbcUrl;
    }

    @Override
    protected TableInDB createTableInDB() {
        return Objects.requireNonNull(this.splitTableStrategy, "SqlServer DataSourceFactory:" + this.identityValue() + " "
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
    protected String getNodeDesc() {
        return Objects.requireNonNull(
                this.splitTableStrategy, "splitTableStrategy can not be null").getNodeDesc();
    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(
            EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {
        return new SqlServerCreateColumnMeta(table, pkCols, columns1, conn);
    }

    @Override
    public String getDBSchema() {
        return this.tabSchema;
    }

    @Override
    protected String getRefectTablesSql() {
        return "select name from sys.tables where is_ms_shipped = 0";
    }

    @Override
    public Optional<String> getEscapeChar() {
        return Optional.of("\"");
    }

    private transient java.sql.Driver driver;

    @Override
    public JDBCConnection createConnection(String jdbcUrl, Optional<Properties> properties, boolean verify) throws SQLException {
        if (driver == null) {
            driver = createDriver();
        }
        java.util.Properties info = properties.orElse(createJdbcProps());

        if (this.userName != null) {
            info.put("user", this.userName);
        }
        if (password != null) {
            info.put("password", password);
        }
//        if (StringUtils.isEmpty(this.timeZone)) {
//            throw new IllegalStateException("property timeZone can not be null");
//        }
        info.put("serverTimezone"
                , Objects.requireNonNull(this.timeZone, "property timeZone can not be null").getTimeZone().getId());
        info.put("characterEncoding", "UTF-8");
        return new SqlServerJDBCConnection(driver.connect(jdbcUrl, info), jdbcUrl, this.getDbName(), this.getDBSchema());
    }

    @Override
    public Optional<ZoneId> getTimeZone() {
        return Optional.of(this.timeZone.getTimeZone());
    }

    protected Properties createJdbcProps() {
        return new Properties();
    }

    protected abstract java.sql.Driver createDriver();


    // @TISExtension
    public static abstract class BasicDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {
        private static final Pattern urlParamsPattern = Pattern.compile("(\\w+?\\=\\w+?)(\\;\\w+?\\=\\w+?)*");
        private static final Pattern pattern_identity = Pattern.compile("[A-Z\\da-z_\\-\\.]+");

        @Override
        protected final String getDataSourceName() {
            return dataSourceName(getVersion());
        }

        protected static String dataSourceName(String version) {
            return DS_TYPE_SQL_SERVER + "-" + version;
        }

        @Override
        public Optional<String> getDefaultDataXReaderDescName() {
            return Optional.of(DataXSqlserverReader.DATAX_NAME);
        }

        protected abstract String getVersion();

        @Override
        public EndType getEndType() {
            return EndType.SqlServer;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        public boolean validateDbName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            Matcher matcher = pattern_identity.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "不符合格式：" + pattern_identity);
                return false;
            }
            return true;
        }

        @Override
        public boolean validateExtraParams(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = urlParamsPattern.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "不符合格式：" + urlParamsPattern);
                return false;
            }
            return true;
        }
    }
}
