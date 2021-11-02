/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.Optional;

/**
 * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-02 11:36
 **/
public class FlinkCDCMongoDBSourceFactory extends MQListenerFactory {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT)
    public String connectionOptions;

    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public String errorsTolerance;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA)
    public String copyExistingPipeline;

    @FormField(ordinal = 3, type = FormFieldType.ENUM)
    public Boolean copyExisting;

    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean errorsLogEnable;

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer copyExistingMaxThreads;

    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer copyExistingQueueSize;

    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer pollMaxBatchSize;

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer pollAwaitTimeMillis;

    @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer heartbeatIntervalMillis;

    private transient IConsumerHandle consumerHandle;

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

    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-MongoDB";
        }

        @Override
        public Optional<IDataXPluginMeta.EndType> getTargetType() {
            return Optional.of(IDataXPluginMeta.EndType.MongoDB);
        }
    }
}
