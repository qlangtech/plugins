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

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.pglike.PGDTOColValProcess.PGCDCTypeVisitor;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 18:14
 **/
public abstract class FlinkCDCPGLikeSourceFactory extends MQListenerFactory {
    private transient IConsumerHandle consumerHandle;

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
    @FormField(ordinal = 2, advance = false, type = FormFieldType.ENUM, validate = {Validator.require})
    public String replicaIdentity;


    public ReplicaIdentity getRepIdentity() {
        return ReplicaIdentity.parse(this.replicaIdentity);
    }


    /**
     * 只支持两种option 'latest' 和 'initial'
     *
     * @return
     * @see org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory #startupOptions()
     */
    //  public StartupOptions getStartupOptions() {
    //   return StartupOptionUtils.getStartupOptions(this.startupOptions);
//        switch (startupOptions) {
//            case StartupOptionUtils.KEY_STARTUP_LATEST:
//                return StartupOptions.latest();
////            case "earliest":
////                return StartupOptions.earliest();
//            case "initial":
//                return StartupOptions.initial();
//            default:
//                throw new IllegalStateException("illegal startupOptions:" + startupOptions);
//        }
    // }


//    @Override
//    public IMQListener create() {
//        FlinkCDCPostgreSQLSourceFunction sourceFunctionCreator = new FlinkCDCPostgreSQLSourceFunction(this);
//        return sourceFunctionCreator;
//    }
    public IConsumerHandle getConsumerHander() {
        Objects.requireNonNull(this.consumerHandle, "prop consumerHandle can not be null");
        return this.consumerHandle;
    }

    @Override
    public void setConsumerHandle(IConsumerHandle consumerHandle) {
        this.consumerHandle = consumerHandle;
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


    }
}
