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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.debuzium.DebuziumPropAssist;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.AbstractPropAssist.Options;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Oracle监听踩坑记： https://mp.weixin.qq.com/s/IQiK7enF5fX0ighRE_i2sg
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:15
 **/
@Public
public class FlinkCDCOracleSourceFactory extends MQListenerFactory {


//     opts.addFieldDescriptor("lob", OracleConnectorConfig.LOB_ENABLED);
//            opts.addFieldDescriptor("poolInterval", OracleConnectorConfig.POLL_INTERVAL_MS);
//            opts.addFieldDescriptor("failureHandle", OracleConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);

    private static Field createLobField() {
        Field old = OracleConnectorConfig.LOB_ENABLED;
        //  String name, String displayName, String description, String defaultValue
        return Field.create(old.name(), "Supports mining LOB fields and operations", old.description(), new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return (Boolean) old.defaultValue();
            }
        });
    }

    public static List<Triple<String, Field, Function<FlinkCDCOracleSourceFactory, Object>>> debeziumProps
            = Lists.newArrayList(
            Triple.of("lob", createLobField(), (sf) -> sf.lob)
            , Triple.of("poolInterval", OracleConnectorConfig.POLL_INTERVAL_MS, (sf) -> sf.poolInterval)
            , Triple.of("failureHandle", OracleConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, (sf) -> sf.failureHandle)
            , Triple.of("miningStrategy", OracleConnectorConfig.LOG_MINING_STRATEGY, (sf) -> sf.miningStrategy));


    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String startupOptions;

//    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String timeZone;

//    public static final Field LOG_MINING_STRATEGY = Field.create("log.mining.strategy")
//            .withDisplayName("Log Mining Strategy")
//            .withEnum(LogMiningStrategy.class, LogMiningStrategy.CATALOG_IN_REDO)
//            .withWidth(Width.MEDIUM)
//            .withImportance(Importance.HIGH)
//            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 8))
//            .withDescription("There are strategies: Online catalog with faster mining but no captured DDL. Another - with data dictio

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String miningStrategy;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean lob;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer poolInterval;

    /**
     * https://debezium.io/documentation/reference/1.9/connectors/oracle.html#oracle-property-event-processing-failure-handling-mode
     */
    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public String failureHandle;


    /**
     * binlog监听在独立的slot中执行
     */
    @FormField(ordinal = 99, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean independentBinLogMonitor;

    @Override
    public final IFlinkColCreator<FlinkCol> createFlinkColCreator(DataSourceMeta sourceMeta) {
        return (meta, colIndex) -> {
            return meta.getType().accept(new OracleCDCTypeVisitor(meta, colIndex));
        };
    }

    StartupOptions getStartupOptions() {
        switch (startupOptions) {
            case "latest":
                return StartupOptions.latest();
            case "initial":
                return StartupOptions.initial();
            default:
                throw new IllegalStateException("illegal startupOptions:" + startupOptions);
        }
    }

    @Override
    public IMQListener create() {
        FlinkCDCOracleSourceFunction sourceFunctionCreator = new FlinkCDCOracleSourceFunction(this);
        return sourceFunctionCreator;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {

        public DefaultDescriptor() {
            super();
            Options<MQListenerFactory, Field> opts = DebuziumPropAssist.createOpts(this);
            for (Triple<String, Field, Function<FlinkCDCOracleSourceFactory, Object>> t : debeziumProps) {
                opts.add(t.getLeft(), t.getMiddle());
            }
        }

        @Override
        public String getDisplayName() {
            return "Flink-CDC-Oracle";
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return IEndTypeGetter.EndType.Oracle;
        }
    }
}
