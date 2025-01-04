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
// */
package com.qlangtech.tis.plugin.kafka.consumer;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.kafka.reader.StartOffset;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;

import java.util.Objects;

import static com.qlangtech.tis.plugin.annotation.Validator.require;


@Public
public class KafkaMQListenerFactory extends MQListenerFactory {

    private transient IConsumerHandle consumerHandle;

    /**
     * binlog监听在独立的slot中执行
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean independentBinLogMonitor;

    @FormField(ordinal = 4, validate = {require})
    public StartOffset startOffset;

//    @FormField(ordinal = 3, validate = {Validator.require})
//    public KafkaSubscriptionMethod subscription;

//    @FormField(validate = {Validator.require, Validator.identity}, ordinal = 2)
//    public String consumeName;
//
//    @FormField(validate = {Validator.require, Validator.identity}, ordinal = 0)
//    public String mqTopic;
//
//    @FormField(validate = {Validator.require, Validator.host}, ordinal = 3)
//    public String namesrvAddr;
//
//
//    public String getConsumeName() {
//        return consumeName;
//    }


    @Override
    public IMQListener create() {
//        if (StringUtils.isEmpty(this.consumeName)) {
//            throw new IllegalStateException("prop consumeName can not be null");
//        }
//        if (StringUtils.isEmpty(this.mqTopic)) {
//            throw new IllegalStateException("prop mqTopic can not be null");
//        }
//        if (StringUtils.isEmpty(this.namesrvAddr)) {
//            throw new IllegalStateException("prop namesrvAddr can not be null");
//        }
//        if (deserialize == null) {
//            throw new IllegalStateException("prop deserialize can not be null");
//        }
//        ConsumerListenerForRm rmListener = new ConsumerListenerForRm();
//        rmListener.setConsumerGroup(this.consumeName);
//        rmListener.setTopic(this.mqTopic);
//        rmListener.setNamesrvAddr(this.namesrvAddr);
//        rmListener.setDeserialize(deserialize);
//        return rmListener;
        return new FlinkKafkaFunction(this);
    }


    @Override
    public  IFlinkColCreator<FlinkCol> createFlinkColCreator() {
        final IFlinkColCreator flinkColCreator = (meta, colIndex) -> {
            return meta.getType().accept(new KafkaCDCTypeVisitor(meta, colIndex));
        };
        return flinkColCreator;
    }

    public static class KafkaCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
        public KafkaCDCTypeVisitor(IColMetaGetter meta, int colIndex) {
            super(meta, colIndex);
        }

//        @Override
//        public FlinkCol varcharType(DataType type) {
//            FlinkCol flinkCol = super.varcharType(type);
//            return flinkCol.setSourceDTOColValProcess(new MySQLStringValueDTOConvert());
//        }
//
//        @Override
//        public FlinkCol blobType(DataType type) {
//            FlinkCol flinkCol = super.blobType(type);
//            return flinkCol.setSourceDTOColValProcess(new MySQLBinaryRawValueDTOConvert());
//        }
    }

    public IConsumerHandle getConsumerHander() {
        return Objects.requireNonNull(consumerHandle, "consumerHandle can not be null");
    }

    @Override
    public void setConsumerHandle(IConsumerHandle consumerHandle) {
        this.consumerHandle = Objects.requireNonNull(consumerHandle, "consumerHandle can not be null");
    }

    @TISExtension(ordinal = 0)
    public static class DefaultDescriptor extends BaseDescriptor {

        @Override
        public String getDisplayName() {
            return getEndType().name();
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.TIS;
        }

        @Override
        public EndType getEndType() {
            return EndType.Kafka;
        }


        // private static final Pattern spec_pattern = Pattern.compile("[\\da-z_]+");

        // private static final Pattern host_pattern = Pattern.compile("[\\da-z]{1}[\\da-z.:]+");

        //  public static final String MSG_HOST_IP_ERROR = "必须由IP、HOST及端口号组成";

        // public static final String MSG_DIGITAL_Alpha_CHARACTER_ERROR = "必须由数字、小写字母、下划线组成";

//        public boolean validateNamesrvAddr(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            Matcher matcher = host_pattern.matcher(value);
//            if (!matcher.matches()) {
//                msgHandler.addFieldError(context, fieldName, MSG_HOST_IP_ERROR);
//                return false;
//            }
//            return true;
//        }

//        public boolean validateMqTopic(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            return validateConsumeName(msgHandler, context, fieldName, value);
//        }
//
//        public boolean validateConsumeName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            Matcher matcher = spec_pattern.matcher(value);
//            if (!matcher.matches()) {
//                msgHandler.addFieldError(context, fieldName, MSG_DIGITAL_Alpha_CHARACTER_ERROR);
//                return false;
//            }
//            return true;
//        }
    }
}
