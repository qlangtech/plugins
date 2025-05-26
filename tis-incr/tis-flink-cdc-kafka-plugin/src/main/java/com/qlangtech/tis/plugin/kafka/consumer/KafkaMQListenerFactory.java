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

import com.qlangtech.plugins.incr.flink.cdc.FlinkCDCPipelineEventProcess;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.RowFieldGetterFactory;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore.Key;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.kafka.reader.DataXKafkaReader;
import com.qlangtech.tis.plugin.datax.kafka.reader.StartOffset;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.BasicFlinkDataMapper.TimestampDataConvert;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.TimestampType;

import java.time.LocalDateTime;
import java.util.Objects;

import static com.qlangtech.tis.plugin.annotation.Validator.require;


@Public
public class KafkaMQListenerFactory extends MQListenerFactory implements KeyedPluginStore.IPluginKeyAware {


    /**
     * binlog监听在独立的slot中执行
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean independentBinLogMonitor;

    @FormField(ordinal = 4, validate = {require})
    public StartOffset startOffset;
    private transient String dataXName;

    @Override
    public void setKey(Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public IMQListener create() {
        return new FlinkKafkaFunction(this);
    }


    @Override
    public IFlinkColCreator<FlinkCol> createFlinkColCreator(DataSourceMeta sourceMeta) {

        if (StringUtils.isEmpty(this.dataXName)) {
            throw new IllegalStateException("pipeline name can not be null");
        }
        DataXKafkaReader dataxReader = (DataXKafkaReader) DataxReader.load(null, this.dataXName);

        final IFlinkColCreator flinkColCreator = (meta, colIndex) -> {
            return meta.getType().accept(new KafkaCDCTypeVisitor(meta, colIndex, dataxReader.format));
        };
        return flinkColCreator;
    }

    public static class KafkaCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
        private final FormatFactory format;

        public KafkaCDCTypeVisitor(IColMetaGetter meta, int colIndex, FormatFactory format) {
            super(meta, colIndex);
            this.format = Objects.requireNonNull(format, "format can not be null");
        }

        @Override
        public FlinkCol dateType(DataType type) {
            return super.dateType(type);
        }

        @Override
        public FlinkCol timestampType(DataType type) {
//            FlinkCol flinkCol = super.timestampType(type);
//            flinkCol.setSourceDTOColValProcess(new KafkaTimestampValueDTOConvert(format));
//            return flinkCol;
            return new FlinkCol(meta, type, new AtomicDataType(new TimestampType(nullable, 3)) //DataTypes.TIMESTAMP(3)
                    , new KafkaTimestampValueDTOConvert(format)
                    , new KafkaDatetimeValueDTOConvert(format)
                    , new FlinkCDCPipelineEventProcess(
                    org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(), new KafkaFlinkCDCPipelineTimestampValueDTOConvert(format))
                    , new RowFieldGetterFactory.TimestampGetter(meta.getName(), colIndex));

        }

        static class KafkaTimestampValueDTOConvert extends KafkaDatetimeValueDTOConvert {
            public KafkaTimestampValueDTOConvert(FormatFactory format) {
                super(format);
            }

            @Override
            public Object apply(Object timestamp) {
                return TimestampData.fromLocalDateTime((LocalDateTime) super.apply(timestamp));
            }
        }

        static class KafkaFlinkCDCPipelineTimestampValueDTOConvert extends KafkaDatetimeValueDTOConvert {
            public KafkaFlinkCDCPipelineTimestampValueDTOConvert(FormatFactory format) {
                super(format);
            }

            @Override
            public Object apply(Object timestamp) {
                return org.apache.flink.cdc.common.data.TimestampData.fromLocalDateTime((LocalDateTime) super.apply(timestamp));
            }
        }

        /**
         * @see TimestampDataConvert
         */
        static class KafkaDatetimeValueDTOConvert extends FlinkCol.DateTimeProcess {
            private final FormatFactory format;

            public KafkaDatetimeValueDTOConvert(FormatFactory format) {
                this.format = Objects.requireNonNull(format, "format can not null");
            }

            @Override
            public Object apply(Object o) {
                LocalDateTime timestamp = null;
                if (o instanceof String) {
                    timestamp = format.parseTimeStamp((String) o);
                } else {
                    timestamp = (LocalDateTime) super.apply(o);
                }
                return timestamp;
            }
        }
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
    }
}
