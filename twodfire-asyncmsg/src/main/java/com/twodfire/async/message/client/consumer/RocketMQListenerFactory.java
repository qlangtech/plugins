/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// */
package com.twodfire.async.message.client.consumer;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.AbstractAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.impl.AbstractMQListenerFactory;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * 基于rockmq的消息监听器，插件实现
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class RocketMQListenerFactory extends AbstractMQListenerFactory {

    @FormField(validate = {Validator.require}, ordinal = 2)
    public String consumeName;

    @FormField(validate = {Validator.require}, ordinal = 0)
    public String mqTopic;

    @FormField(validate = {Validator.require}, ordinal = 3)
    public String namesrvAddr;

    @FormField(validate = {Validator.require}, ordinal = 1)
    public AbstractAsyncMsgDeserialize deserialize;

    public String getConsumeName() {
        return consumeName;
    }

    private final String consumerHandle = "default";

    public static void main(String[] args) {
        // System.out.println(spec_pattern.matcher("c_otter_binlogorder_solr").matches());
        //
        // System.out.println(host_pattern.matcher("10.1.21.148:9876").matches());
    }

    @Override
    public IMQListener create() {
        if (StringUtils.isEmpty(this.consumeName)) {
            throw new IllegalStateException("prop consumeName can not be null");
        }
        if (StringUtils.isEmpty(this.mqTopic)) {
            throw new IllegalStateException("prop mqTopic can not be null");
        }
        if (StringUtils.isEmpty(this.namesrvAddr)) {
            throw new IllegalStateException("prop namesrvAddr can not be null");
        }
        if (deserialize == null) {
            throw new IllegalStateException("prop deserialize can not be null");
        }
        ConsumerListenerForRm rmListener = new ConsumerListenerForRm();
        rmListener.setConsumerGroup(this.consumeName);
        rmListener.setTopic(this.mqTopic);
        rmListener.setNamesrvAddr(this.namesrvAddr);
        rmListener.setDeserialize(deserialize);
        // rmListener.setConsumerHandle();
        return rmListener;
    }

    // @JSONField(serialize = false)
    // public IConsumerHandle getConsumeHandle() {
    // 
    // Optional<AbstractConsumerHandle> first = AbstractConsumerHandle.all().stream().filter((r)
    // -> StringUtils.equals(r.getName(), this.consumerHandle)).findFirst();
    // if (!first.isPresent()) {
    // throw new IllegalStateException("consumerHandle:" + this.consumerHandle + " can not find relevant plugin");
    // }
    // return first.get();
    // }
    public void setConsumeName(String consumeName) {
        this.consumeName = consumeName;
    }

    public String getMqTopic() {
        return mqTopic;
    }

    public void setMqTopic(String mqTopic) {
        this.mqTopic = mqTopic;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public AbstractAsyncMsgDeserialize getDeserialize() {
        return deserialize;
    }

    public void setDeserialize(AbstractAsyncMsgDeserialize deserialize) {
        this.deserialize = deserialize;
    }

    @TISExtension(ordinal = 0)
    public static class DefaultDescriptor extends Descriptor<AbstractMQListenerFactory> {

        @Override
        public String getDisplayName() {
            return "RocketMq";
        }

        private static final Pattern spec_pattern = Pattern.compile("[\\da-z_]+");

        private static final Pattern host_pattern = Pattern.compile("[\\da-z]{1}[\\da-z.:]+");

        public static final String MSG_HOST_IP_ERROR = "必须由IP、HOST及端口号组成";

        public static final String MSG_DIGITAL_Alpha_CHARACTER_ERROR = "必须由数字、小写字母、下划线组成";

        public boolean validateNamesrvAddr(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = host_pattern.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, MSG_HOST_IP_ERROR);
                return false;
            }
            return true;
        }

        public boolean validateMqTopic(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateConsumeName(msgHandler, context, fieldName, value);
        }

        public boolean validateConsumeName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = spec_pattern.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, MSG_DIGITAL_Alpha_CHARACTER_ERROR);
                return false;
            }
            return true;
        }
    }
}
