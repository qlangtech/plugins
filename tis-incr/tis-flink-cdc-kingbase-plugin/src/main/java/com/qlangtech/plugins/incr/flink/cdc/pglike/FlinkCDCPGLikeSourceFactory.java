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

package com.qlangtech.plugins.incr.flink.cdc.pglike;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.debuzium.DebuziumPropAssist;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.pglike.PGDTOColValProcess.PGCDCTypeVisitor;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 18:14
 **/
public abstract class FlinkCDCPGLikeSourceFactory extends MQListenerFactory {

    public static final String FIELD_REPLICA_RULE = "replicaRule";
    public static final String FIELD_KEY_SLOT_NAME = "slotName";
    /**
     * The name of the Postgres logical decoding plug-in installed on the server. Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming, wal2json_rds_streaming and pgoutput.
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String decodingPluginName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String slotName;

    //DROP_SLOT_ON_STOP
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require, Validator.db_col_name})
    public Boolean dropSolt;

    /**
     * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-optionshttps://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String startupOptions;
    // REPLICA IDENTITY
    @FormField(ordinal = 4, advance = false, validate = {Validator.require})
    public PGLikeReplicaIdentity replicaRule;


    public final PGLikeReplicaIdentity getRepIdentity() {
        return Objects.requireNonNull(replicaRule, "replicaIdentity can not be null");
    }


    @Override
    public IFlinkColCreator<FlinkCol> createFlinkColCreator(DataSourceMeta sourceMeta) {
        IFlinkColCreator<FlinkCol> flinkColCreator = (meta, colIndex) -> {
            return meta.getType().accept(new PGCDCTypeVisitor(meta, colIndex));
        };
        return flinkColCreator;
    }

//    public static List<Triple<String, Field, Function<FlinkCDCPGLikeSourceFactory, Object>>> debeziumProps
//            = Lists.newArrayList(
//            Triple.of(FIELD_KEY_SLOT_NAME, PostgresConnectorConfig.SLOT_NAME, (sf) -> sf.slotName)
//            , Triple.of("dropSolt", PostgresConnectorConfig.DROP_SLOT_ON_STOP, (sf) -> sf.dropSolt));

    // Properties debeziumProperties
    public static List<Pair<Consumer<AbstractPropAssist.Options<MQListenerFactory, Field>>, BiConsumer<Properties, FlinkCDCPGLikeSourceFactory>>> debeziumProps
            = Lists.newArrayList(
            Pair.of((opts) -> {
                        opts.add(FIELD_KEY_SLOT_NAME, PostgresConnectorConfig.SLOT_NAME
                                , new OverwriteProps().setDftVal(IPluginContext.getThreadLocalInstance().getCollectionName().getPipelineName()));
                    }
                    , (debeziumProperties, sourceFactory) -> {
                        debeziumProperties.setProperty(PostgresConnectorConfig.SLOT_NAME.name(), sourceFactory.slotName);
                    })
            , //
            Pair.of((opts) -> {
                opts.add("dropSolt", PostgresConnectorConfig.DROP_SLOT_ON_STOP);
            }, (debeziumProperties, sourceFactory) -> {
                debeziumProperties.setProperty(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), String.valueOf(sourceFactory.dropSolt));
            }));

    //  @TISExtension()
    public static class BasePGLikeDescriptor extends MQListenerFactory.BaseDescriptor {


        public BasePGLikeDescriptor() {
            super();
            AbstractPropAssist.Options<MQListenerFactory, Field> opts = DebuziumPropAssist.createOpts(this);
            debeziumProps.forEach((pair) -> {
                //opts.add(trip.getLeft(), trip.getMiddle());
                pair.getKey().accept(opts);
            });
//            this.opts.add("slotName", PostgresConnectorConfig.SLOT_NAME);
//            this.opts.add("dropSolt", PostgresConnectorConfig.DROP_SLOT_ON_STOP);
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        // @Override
        public boolean validateSlotName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (!value.matches("[a-z0-9_]{1,63}")) {
                msgHandler.addFieldError(context, fieldName, "must contain only digits, lowercase characters and underscores with length <= 63");
                return false;
            }

            return true;
        }

        @Override
        protected boolean validateMQListenerForm(
                IControlMsgHandler msgHandler, Context context, MQListenerFactory sourceFactory) {
            FlinkCDCPGLikeSourceFactory incrSource = (FlinkCDCPGLikeSourceFactory) sourceFactory;
            DataXName pipeline = msgHandler.getCollectionName();
            DataxReader dataxReader = DataxReader.load((IPluginContext) msgHandler, pipeline.getPipelineName());
            IDataSourceFactoryGetter dataSourceGetter = (IDataSourceFactoryGetter) dataxReader;
            List<ISelectedTab> selectedTabs = dataxReader.getSelectedTabs();

            if (!incrSource.replicaRule.validateSelectedTabs(msgHandler, context, dataSourceGetter, selectedTabs)) {
                return false;
            }

            final boolean[] validationPassed = {true};

            dataSourceGetter.getDataSourceFactory().visitFirstConnection((conn) -> {
                // Validate replication slot availability
                String slotName = incrSource.slotName;

                String checkSlotSql = "SELECT active FROM pg_replication_slots WHERE slot_name = ?";
                try (java.sql.PreparedStatement pstmt = conn.preparedStatement(checkSlotSql)) {
                    pstmt.setString(1, slotName);

                    try (java.sql.ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            boolean isActive = rs.getBoolean("active");

                            if (isActive) {
                                // Slot is currently active and being used by another task
                                msgHandler.addFieldError(context, FIELD_KEY_SLOT_NAME,
                                        "Replication slot '" + slotName + "' is already active and in use by another CDC task. " +
                                                "Please use a different slot name or stop the task currently using this slot.");
                                validationPassed[0] = false;
                            }
                            // If active=false, the slot exists but is idle, which is acceptable
                            // The task can reuse this slot
                        }
                        // If no result, the slot doesn't exist yet, which is fine
                        // Flink CDC will create the slot automatically on startup
                    }
                } catch (java.sql.SQLException e) {
                    // Log warning but don't fail validation if we can't check the slot
                    // This could happen if pg_replication_slots view doesn't exist or permission issues
                    msgHandler.addErrorMessage(context,
                            "Unable to validate replication slot status: " + e.getMessage());
                    validationPassed[0] = false;
                }
            });

            return validationPassed[0];
        }
    }
}
