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

package com.qlangtech.plugins.incr.flink.cdc.postgresql;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.cdc.pglike.FlinkCDCPGLikeSourceFactory;
import com.qlangtech.plugins.incr.flink.cdc.postgresql.PostgreSQLCDCValidator.ValidationResult;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:15
 **/
@Public
public class FlinkCDCPostreSQLSourceFactory extends FlinkCDCPGLikeSourceFactory {
    private static final Logger logger = LoggerFactory.getLogger(FlinkCDCPostreSQLSourceFactory.class);

    @Override
    public IMQListener create() {
        return new FlinkCDCPostgreSQLSourceFunction(this);
    }

//    The name of the Postgres logical decoding plug-in installed on the server. Supported
//         * values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,
//            * wal2json_rds_streaming and pgoutput.


    @TISExtension()
    public static class DefaultDescriptor extends FlinkCDCPGLikeSourceFactory.BasePGLikeDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-PostgreSQL";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateMQListenerForm(msgHandler, context, postFormVals.newInstance());
        }

        @Override
        protected boolean validateMQListenerForm(IControlMsgHandler msgHandler, Context context, MQListenerFactory postFormVals) {
            IPluginContext plugContext = (IPluginContext) msgHandler;
            if (!plugContext.isCollectionAware()) {
                throw new IllegalStateException("plugContext must be collection aware");
            }
            DataXName dataXName = plugContext.getCollectionName();
            DataxReader dataxReader = DataxReader.load(plugContext, dataXName.getPipelineName());
            DataSourceFactory dsFactory = ((IDataSourceFactoryGetter) dataxReader).getDataSourceFactory();

            // 获取选中的表列表
            List<ISelectedTab> tabs = dataxReader.getSelectedTabs();
            List<String> tableNames = tabs.stream()
                    .map(ISelectedTab::getName)
                    .collect(Collectors.toList());

            FlinkCDCPostreSQLSourceFactory sourceFactory = (FlinkCDCPostreSQLSourceFactory) postFormVals;

            // 执行PostgreSQL CDC先验校验
            final boolean[] validationPassed = {true};

            dsFactory.visitFirstConnection(new DataSourceFactory.IConnProcessor() {
                @Override
                public void vist(JDBCConnection conn) throws SQLException, TableNotFoundException {
                    Connection connection = conn.getConnection();

                    logger.info("Starting PostgreSQL CDC validation, table count: {}", tableNames.size());

                    // 调用校验器进行完整校验
                    ValidationResult result = PostgreSQLCDCValidator.validate(
                            connection,
                            sourceFactory,
                            tableNames
                    );

                    // 将校验错误添加到msgHandler,显示到前端
                    if (!result.isValid()) {
                        validationPassed[0] = false;
                        for (String error : result.getErrors()) {
                            msgHandler.addErrorMessage(context, error);
                        }
                        logger.error("PostgreSQL CDC validation failed, error count: {}", result.getErrors().size());
                    }

                    // 记录警告信息到日志
                    for (String warning : result.getWarnings()) {
                        logger.warn("PostgreSQL CDC validation warning: {}", warning);
                    }

                    if (result.isValid()) {
                        logger.info("PostgreSQL CDC validation passed");
                    }
                }
            });

            return validationPassed[0];
        }

        @Override
        public EndType getEndType() {
            return EndType.Postgres;
        }
    }

}
