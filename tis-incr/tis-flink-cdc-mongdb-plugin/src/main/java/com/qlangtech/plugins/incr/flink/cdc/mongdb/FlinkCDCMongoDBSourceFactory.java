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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.time.ZoneId;

/**
 * https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-02 11:36
 **/
@Public
public class FlinkCDCMongoDBSourceFactory extends MQListenerFactory {

    @FormField(ordinal = 1, validate = {Validator.require})
    public MongoCDCStartupOptions startupOption;
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String timeZone;
    /**
     * https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/#full-changeloga-namefull-changelog-id003-a
     */
    @FormField(ordinal = 9, validate = {Validator.require})
    public UpdateRecordComplete updateRecordComplete;


    public ZoneId parseZoneId() {
        return ZoneId.of(timeZone);
    }

    @FormField(ordinal = 10, advance = true, type = FormFieldType.INPUTTEXT)
    public String connectionOptions;

    @FormField(ordinal = 11, advance = true, type = FormFieldType.TEXTAREA)
    public String copyExistingPipeline;

    private transient IConsumerHandle consumerHandle;

    @Override
    public IFlinkColCreator<FlinkCol> createFlinkColCreator() {
        return (meta, colIndex) -> {
            return meta.getType().accept(new MongoDBCDCTypeVisitor(meta, colIndex, this.parseZoneId()));
        };
    }

    @Override
    public IMQListener create() {
        FlinkCDCMongoDBSourceFunction sourceFunction = new FlinkCDCMongoDBSourceFunction(this);
        return sourceFunction;
    }

    @Override
    public void setConsumerHandle(IConsumerHandle consumerHandle) {
        this.consumerHandle = consumerHandle;
    }

    public IConsumerHandle getConsumerHander() {
        return this.consumerHandle;
    }


    /**
     * 还没有调试好暂时不支持
     */
    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-MongoDB";
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return IEndTypeGetter.EndType.MongoDB;
        }
    }
}
