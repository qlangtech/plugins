/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugins.incr.flink.chunjun.source;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTabExtend;
import com.qlangtech.tis.plugin.incr.ISelectedTabExtendFactory;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.table.data.RowData;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-10 11:58
 **/
public abstract class ChunjunSourceFactory extends MQListenerFactory {

    private static final String DESC_NAME = "Flink-Chunjun-";
    private transient IConsumerHandle<RowData, JobExecutionResult> consumerHandle;

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer fetchSize;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer queryTimeOut;

    public IConsumerHandle getConsumerHandle() {
        return this.consumerHandle;
    }


//    @Override
//    public void setConsumerHandle(IConsumerHandle consumerHandle) {
//        this.consumerHandle = consumerHandle;
//    }

    @Override
    public Descriptor<MQListenerFactory> getDescriptor() {
        Descriptor<MQListenerFactory> desc = super.getDescriptor();
        if (!(desc instanceof BaseChunjunDescriptor)) {
            throw new IllegalStateException("desc must be type of " + BaseChunjunDescriptor.class);
        }
        return desc;
    }


    public static abstract class BaseChunjunDescriptor
            extends BaseDescriptor implements ISelectedTabExtendFactory {
        @Override
        public String getDisplayName() {
            return DESC_NAME + getEndType().name();
        }
        @Override
        public final PluginVender getVender() {
            return PluginVender.CHUNJUN;
        }
        @Override
        public Descriptor<SelectedTabExtend> getSelectedTableExtendDescriptor() {
            return TIS.get().getDescriptor(SelectedTabPropsExtends.class);
        }
    }
}
