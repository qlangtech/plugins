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

package com.qlangtech.tis.plugin.datax.kafka.reader.offsets;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.kafka.reader.StartOffset;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist <br/>
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/#starting-offset
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-27 21:28
 **/
public class EarliestWhenNoneCommittedOffsets extends StartOffset {
    @Override
    public void setOffset(KafkaSourceBuilder<DTO> kafkaSourceBuilder) {
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST));
    }

    @TISExtension
    public static final class DftDescriptor extends Descriptor<StartOffset> {
        public DftDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Earliest When None Committed Offset";
        }
    }
}
