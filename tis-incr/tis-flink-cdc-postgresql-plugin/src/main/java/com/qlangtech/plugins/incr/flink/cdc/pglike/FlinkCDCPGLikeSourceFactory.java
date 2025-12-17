///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.qlangtech.plugins.incr.flink.cdc.pglike;
//
//import com.alibaba.citrus.turbine.Context;
//import com.google.common.collect.Lists;
//import com.qlangtech.plugins.incr.debuzium.DebuziumPropAssist;
//import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
//import com.qlangtech.plugins.incr.flink.cdc.pglike.PGDTOColValProcess.PGCDCTypeVisitor;
//import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
//import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
//import com.qlangtech.tis.extension.util.AbstractPropAssist;
//import com.qlangtech.tis.extension.util.OverwriteProps;
//import com.qlangtech.tis.plugin.annotation.FormField;
//import com.qlangtech.tis.plugin.annotation.FormFieldType;
//import com.qlangtech.tis.plugin.annotation.Validator;
//import com.qlangtech.tis.plugin.ds.DataSourceMeta;
//import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
//import com.qlangtech.tis.util.IPluginContext;
//import io.debezium.config.Field;
//import io.debezium.connector.postgresql.PostgresConnectorConfig;
//import org.apache.commons.lang3.tuple.Pair;
//
//import java.util.List;
//import java.util.Properties;
//import java.util.function.BiConsumer;
//import java.util.function.Consumer;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-01-19 18:14
// **/
//public abstract class FlinkCDCPGLikeSourceFactory extends MQListenerFactory {
//    //private transient IConsumerHandle consumerHandle;
//    public static final String FIELD_KEY_SLOT_NAME = "slotName";
//    /**
//     * The name of the Postgres logical decoding plug-in installed on the server. Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming, wal2json_rds_streaming and pgoutput.
//     */
//    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String decodingPluginName;
//
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
//    public String slotName;
//
//    //DROP_SLOT_ON_STOP
//    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require, Validator.db_col_name})
//    public Boolean dropSolt;
//
//    /**
//     * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-optionshttps://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options
//     */
//    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String startupOptions;
//    // REPLICA IDENTITY
//    @FormField(ordinal = 4, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String replicaIdentity;
//
//
//    public ReplicaIdentity getRepIdentity() {
//        return ReplicaIdentity.parse(this.replicaIdentity);
//    }
//
//
//    /**
//     * 只支持两种option 'latest' 和 'initial'
//     *
//     * @return
//     * @see org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory #startupOptions()
//     */
//
//
//    @Override
//    public IFlinkColCreator<FlinkCol> createFlinkColCreator(DataSourceMeta sourceMeta) {
//        IFlinkColCreator<FlinkCol> flinkColCreator = (meta, colIndex) -> {
//            return meta.getType().accept(new PGCDCTypeVisitor(meta, colIndex));
//        };
//        return flinkColCreator;
//    }
//
//    public static List<Pair<Consumer<AbstractPropAssist.Options<MQListenerFactory, Field>>, BiConsumer<Properties, FlinkCDCPGLikeSourceFactory>>> debeziumProps
//            = Lists.newArrayList(
//            Pair.of((opts) -> {
//                        opts.add(FIELD_KEY_SLOT_NAME, PostgresConnectorConfig.SLOT_NAME
//                                , new OverwriteProps().setDftVal(IPluginContext.getThreadLocalInstance().getCollectionName().getPipelineName()));
//                    }
//                    , (debeziumProperties, sourceFactory) -> {
//                        debeziumProperties.setProperty(PostgresConnectorConfig.SLOT_NAME.name(), sourceFactory.slotName);
//                    })
//            , //
//            Pair.of((opts) -> {
//                opts.add("dropSolt", PostgresConnectorConfig.DROP_SLOT_ON_STOP);
//            }, (debeziumProperties, sourceFactory) -> {
//                debeziumProperties.setProperty(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), String.valueOf(sourceFactory.dropSolt));
//            }));
//
//
//    //  @TISExtension()
//    public static class BasePGLikeDescriptor extends BaseDescriptor {
//        public BasePGLikeDescriptor() {
//            super();
//            AbstractPropAssist.Options<MQListenerFactory, Field> opts = DebuziumPropAssist.createOpts(this);
//            debeziumProps.forEach((pair) -> {
//                //opts.add(trip.getLeft(), trip.getMiddle());
//                pair.getKey().accept(opts);
//            });
//        }
//        public boolean validateSlotName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            if (!value.matches("[a-z0-9_]{1,63}")) {
//                msgHandler.addFieldError(context, fieldName, "must contain only digits, lowercase characters and underscores with length <= 63");
//                return false;
//            }
//            return true;
//        }
//        @Override
//        public PluginVender getVender() {
//            return PluginVender.FLINK_CDC;
//        }
//
//
//    }
//}
