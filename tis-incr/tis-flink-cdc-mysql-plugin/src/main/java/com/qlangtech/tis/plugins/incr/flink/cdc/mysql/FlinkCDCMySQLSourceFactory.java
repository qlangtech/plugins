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

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel.HostDbs;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.cdc.mysql.FlinkCDCMysqlSourceFunction.MySQLReaderSourceCreator;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import com.ververica.cdc.connectors.mysql.MySqlValidator;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.api.ValidationException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:15
 **/
@Public
public class FlinkCDCMySQLSourceFactory extends MQListenerFactory {
    private transient IConsumerHandle consumerHandle;

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public com.qlangtech.tis.plugins.incr.flink.cdc.mysql.startup.StartupOptions startupOptions;

    /**
     * binlog监听在独立的slot中执行
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean independentBinLogMonitor;

    StartupOptions getStartupOptions() {
        return startupOptions.getOptionsType();
//        switch (startupOptions) {
//            case "latest":
//                return StartupOptions.latest();
//            case "earliest":
//                return StartupOptions.earliest();
//            case "initial":
//                return StartupOptions.initial();
//            default:
//                throw new IllegalStateException("illegal startupOptions:" + startupOptions);
//        }
    }

    @Override
    public IMQListener create() {
        FlinkCDCMysqlSourceFunction sourceFunctionCreator = new FlinkCDCMysqlSourceFunction(this);
        return sourceFunctionCreator;
    }

    public IConsumerHandle getConsumerHander() {
        Objects.requireNonNull(this.consumerHandle, "prop consumerHandle can not be null");
        return this.consumerHandle;
    }

    @Override
    public void setConsumerHandle(IConsumerHandle consumerHandle) {
        this.consumerHandle = consumerHandle;
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-MySQL";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {


            IPluginContext plugContext = (IPluginContext) msgHandler;
            if (!plugContext.isCollectionAware()) {
                throw new IllegalStateException("plugContext must be collection aware");
            }

            DataxReader dataxReader = DataxReader.load(plugContext, plugContext.getCollectionName());

            List<ISelectedTab> tabs = dataxReader.getSelectedTabs();
            DataSourceFactory dsFactory = ((IDataSourceFactoryGetter) dataxReader).getDataSourceFactory();
            FlinkCDCMySQLSourceFactory sourceFactory = postFormVals.newInstance();

            AtomicInteger count = new AtomicInteger();
            /**
             * 对MySQL进行校验，这样可以在任务定义时候就可以报错避免在flink运行期出错<br>
             * https://github.com/datavane/tis/issues/306
             */
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                /**
                 * for method io.debezium.config.Configuration.getInstance() will get execute Thread.currentThread().getContextClassLoader()
                 */
                Thread.currentThread().setContextClassLoader(FlinkCDCMySQLSourceFactory.class.getClassLoader());
                SourceChannel.getSourceFunction(
                        dsFactory,
                        tabs,
                        new MySQLReaderSourceCreator((BasicDataSourceFactory) dsFactory, sourceFactory) {
                            @Override
                            protected List<ReaderSource> createReaderSources(String dbHost, HostDbs dbs, MySqlSource<DTO> sourceFunc) {
                                if (count.getAndIncrement() > 0) {
                                    return null;
                                }
                                try {
                                    MySqlValidator validator = new MySqlValidator(sourceFunc.getConfigFactory().createConfig(1));
                                    validator.validate();
                                } catch (Exception e) {

                                    for (Throwable t : ExceptionUtils.getThrowableList(e)) {
                                        if (ValidationException.class.isAssignableFrom(t.getClass())) {
                                            throw TisException.create(t.getMessage(), t);
                                        }
                                    }

                                    throw e;
                                }

                                return null;
                            }
                        }
                );
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
            return true;
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return IEndTypeGetter.EndType.MySQL;
        }

//        @Override
//        public Optional<IEndTypeGetter.EndType> getTargetType() {
//            return Optional.of(IEndTypeGetter.EndType.MySQL);
//        }
    }
}
