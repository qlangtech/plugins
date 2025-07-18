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

package com.qlangtech.plugins.incr.flink.cdc.kingbase;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.kingbase.source.KingBaseDialect;
import com.qlangtech.plugins.incr.flink.cdc.pglike.FlinkCDCPGLikeSourceFunction;
import com.qlangtech.plugins.incr.flink.cdc.pglike.PostgreSQLDeserializationSchema;
import com.qlangtech.plugins.incr.flink.cdc.pglike.StartupOptionUtils;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DBConfig.IProcess;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.ISchemaSupported;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.realtime.transfer.DTO;
import io.debezium.jdbc.JdbcConfiguration;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 19:11
 **/
public class FlinkCDCKingBaseSourceFunction extends FlinkCDCPGLikeSourceFunction {

    public FlinkCDCKingBaseSourceFunction(FlinkCDCKingBaseSourceFactory sourceFactory) {
        super(sourceFactory);
    }

    @Override
    protected String getWALDecoderPluginName() {
        return Objects.requireNonNull(sourceFactory, "sourceFactory can not be null").decodingPluginName;
    }

    @Override
    protected PostgresIncrementalSource<DTO> createIncrementalSource(
            List<ISelectedTab> tabs, String dbHost, Set<String> tbs
            , Properties debeziumProperties, String dbname, BasicDataSourceFactory dsFactory
            , ISchemaSupported schemaSupported, IFlinkColCreator<FlinkCol> flinkColCreator
            , Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper) {


        debeziumProperties.setProperty(
                DATABASE_CONFIG_PREFIX + JdbcConfiguration.CONNECTION_FACTORY_CLASS.name()
                , com.qlangtech.plugins.incr.flink.cdc.kingbase.source.KingBaseConnectionFactory.class.getCanonicalName());
        boolean[] hasSetUrl = new boolean[1];
        try {
            dsFactory.getDbConfig().vistDbName(new IProcess() {
                @Override
                public boolean visit(DBConfig config, String jdbcUrl, String ip, String dbName) throws Exception {
                    // used for KingBaseConnectionFactory
                    debeziumProperties.setProperty(DATABASE_CONFIG_PREFIX + DataxUtils.DATASOURCE_FACTORY_IDENTITY
                            , Objects.requireNonNull(dsFactory.identityValue(), "datasource identity can not be null"));
                    debeziumProperties.setProperty(DATABASE_CONFIG_PREFIX + DataxUtils.DATASOURCE_JDBC_URL, jdbcUrl);
                    return hasSetUrl[0] = true;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!hasSetUrl[0]) {
            throw new IllegalStateException("jdbc url has not been set");
        }

        final PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();


        configFactory.hostname(dbHost)
                .port(dsFactory.port);
        configFactory.database(dbname); // monitor postgres database
        configFactory.schemaList(new String[]{schemaSupported.getDBSchema()});  // monitor inventory schema
        configFactory.tableList(tbs.toArray(new String[tbs.size()])) // monitor products table
                // .tableList("tis.base")
                .username(dsFactory.userName);
        configFactory.decodingPluginName(sourceFactory.decodingPluginName);
        configFactory.password(dsFactory.password);
        configFactory.debeziumProperties(debeziumProperties);

        configFactory.startupOptions(StartupOptionUtils.getStartupOptions(sourceFactory.startupOptions));
//             .deserializer(new PostgreSQLDeserializationSchema(tabs, flinkColCreator, contextParamValsGetterMapper, sourceFactory.getRepIdentity()))
//                .build();


        PostgreSQLDeserializationSchema deserializer = new PostgreSQLDeserializationSchema(tabs, flinkColCreator, contextParamValsGetterMapper, sourceFactory.getRepIdentity());

        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        KingBaseDialect dialect = new KingBaseDialect(configFactory.create(0));


        return new PostgresIncrementalSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }
}
