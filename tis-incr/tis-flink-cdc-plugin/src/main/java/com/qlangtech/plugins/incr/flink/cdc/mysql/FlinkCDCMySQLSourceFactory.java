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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:15
 **/
public class FlinkCDCMySQLSourceFactory extends MQListenerFactory {
    private transient IConsumerHandle consumerHandle;

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String startupOptions;

    StartupOptions getStartupOptions() {
        switch (startupOptions) {
            case "latest":
                return StartupOptions.latest();
            case "earliest":
                return StartupOptions.earliest();
            case "initial":
                return StartupOptions.initial();
            default:
                throw new IllegalStateException("illegal startupOptions:" + startupOptions);
        }
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
    public static class DefaultDescriptor extends Descriptor<MQListenerFactory> {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-MySQL";
        }
    }
}
