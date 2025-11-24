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
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.pglike.PGDTOColValProcess.PGCDCTypeVisitor;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 18:14
 **/
public abstract class FlinkCDCPGLikeSourceFactory extends MQListenerFactory {

    public static final String FIELD_REPLICA_RULE = "replicaRule";

    /**
     * The name of the Postgres logical decoding plug-in installed on the server. Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming, wal2json_rds_streaming and pgoutput.
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String decodingPluginName;

    /**
     * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-optionshttps://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String startupOptions;
    // REPLICA IDENTITY
    @FormField(ordinal = 2, advance = false, validate = {Validator.require})
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

    //  @TISExtension()
    public static class BasePGLikeDescriptor extends MQListenerFactory.BaseDescriptor {

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        @Override
        protected boolean validateMQListenerForm(
                IControlMsgHandler msgHandler, Context context, MQListenerFactory sourceFactory) {
            FlinkCDCPGLikeSourceFactory incrSource = (FlinkCDCPGLikeSourceFactory) sourceFactory;
            DataXName pipeline = msgHandler.getCollectionName();
            DataxReader dataxReader = DataxReader.load((IPluginContext) msgHandler, pipeline.getPipelineName());
            IDataSourceFactoryGetter dataSourceGetter = (IDataSourceFactoryGetter) dataxReader;
            List<ISelectedTab> selectedTabs = dataxReader.getSelectedTabs();
            return incrSource.replicaRule.validateSelectedTabs(msgHandler, context, dataSourceGetter, selectedTabs);
        }
    }
}
