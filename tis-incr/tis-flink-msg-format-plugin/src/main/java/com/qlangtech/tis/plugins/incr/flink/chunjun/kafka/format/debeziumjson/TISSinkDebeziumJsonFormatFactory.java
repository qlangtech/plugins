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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.debeziumjson;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.TISFlinkProp;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.debezium.DebeziumJsonFormatFactory;
import org.apache.flink.formats.json.debezium.DebeziumJsonFormatOptions;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

import java.util.function.BiFunction;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/formats/debezium/ <br/>
 * https://debezium.io/documentation/reference/3.0/tutorial.html#viewing-change-events <br/>
 * <pre>
 *     {
 * 	"before": null,
 * 	"after": {
 * 		"order_id": "999331946a871d3a016a8aecf4c00197",
 * 		"global_code": "201905051630410104437",
 * 		"simple_code": "0104437",
 * 		"seat_code": "104",
 * 		"code": 2,
 * 		"curr_date": "2019-05-06",
 * 		"totalpay_id": "999331946a871d3a016a8aecf4bd0194",
 * 		"seat_id": "999331945e835e4a015e845261e10031",
 * 		"people_count": 4,
 * 		"open_time": 1557108946107,
 * 		"status": 4,
 * 		"memo": null,
 * 		"inner_code": "201905060002",
 * 		"menutime_id": "99933194606d64b101606e302f2f2464",
 * 		"worker_id": "f703b0c370774d6aacf98ba305c268a0",
 * 		"end_time": 1557113106182,
 * 		"feeplan_id": null,
 * 		"op_user_id": "f703b0c370774d6aacf98ba305c268a0",
 * 		"order_from": 0,
 * 		"order_kind": 1,
 * 		"area_id": "e77f04c1f10241c79f8b7ee22e657896",
 * 		"name": null,
 * 		"mobile": "",
 * 		"tel": null,
 * 		"is_autocommit": 0,
 * 		"send_time": 0,
 * 		"address": null,
 * 		"paymode": 0,
 * 		"outfee": 0,
 * 		"sender_id": null,
 * 		"customerregister_id": null,
 * 		"waitingorder_id": null,
 * 		"send_status": 0,
 * 		"audit_status": null,
 * 		"is_hide": 0,
 * 		"entity_id": "99933194",
 * 		"is_valid": 1,
 * 		"create_time": 1557108946112,
 * 		"op_time": 1557113106184,
 * 		"last_ver": 4,
 * 		"load_time": 1557108946,
 * 		"modify_time": 1557113116,
 * 		"is_limittime": false,
 * 		"scan_url": "http://api.l.whereask.com/ma/order/201905051630410104437",
 * 		"seat_mark": "",
 * 		"reservetime_id": "",
 * 		"is_wait": 0,
 * 		"is_print": 1,
 * 		"book_id": "",
 * 		"reserve_id": "",
 * 		"orign_id": "",
 * 		"reserve_status": 0,
 * 		"ext": "{\"clientFrom\":0,\"collectPayMode\":0,\"dinnerType\":0,\"industryCode\":0,\"isPrePay\":0,\"isReverseCheckout\":0,\"mealMark\":\"\",\"mergeMode\":0,\"posNumber\":\"CNDFPBP10S17010100104\"}"
 *        },
 * 	"op": "c",
 * 	"source": {
 * 		"db": "order2",
 * 		"table": "orderdetail_01"
 *    }
 * }
 *
 * </pre>
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-15 12:25
 **/
public class TISSinkDebeziumJsonFormatFactory extends FormatFactory {
//    @FormField(ordinal = 0, type = FormFieldType.ENUM, advance = true)
//    public Boolean schemaInclude;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, advance = true)
    public Boolean ignoreParseErrors;
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = Validator.require)
    public String timestampFormat;

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, advance = true)
    public String nullKeyMode;
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, advance = true)
    public String nullKeyLiteral;
    @FormField(ordinal = 6, type = FormFieldType.ENUM, advance = true)
    public Boolean encodeDecimal;

    @Override
    public boolean validateFormtField(IControlMsgHandler msgHandler, Context context, String fieldName, DataxReader dataxReader) {
        return true;
    }

    @Override
    protected String getTimestampFormat() {
        return this.timestampFormat;
    }

    @Override
    public String getNullFormat() {
        return this.nullKeyLiteral;
    }

    @Override
    public KafkaStructuredRecord parseRecord(KafkaStructuredRecord reuse, byte[] record) {
        throw new UnsupportedOperationException();
    }


    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(String targetTableName) {
        return createFormat(targetTableName, (factory, cfg) -> {
            return factory.createEncodingFormat(null, cfg);
        });
    }

    protected <T> T createFormat(String targetTabName
            , BiFunction<DebeziumJsonFormatFactory, Configuration, T> formatCreator) {
        DebeziumJsonFormatFactory debeziumJsonFormatFactory = new DebeziumJsonFormatFactory();

        TISSinkDebeziumJsonFormatFactory.DftDescriptor desc = (TISSinkDebeziumJsonFormatFactory.DftDescriptor) this.getDescriptor();
        return formatCreator.apply(debeziumJsonFormatFactory
                , desc.options.createFlinkCfg(this)
                        .set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));

//        return canalFormatFactory.createEncodingFormat(null
//                , desc.options.createFlinkCfg(this).set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));
    }

    @Override
    public boolean acceptMultipleTable() {
        return true;
    }

//    @Override
//    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(String targetTabName) {
//        throw new UnsupportedOperationException("createDecodingFormat");
//    }

    @TISExtension
    public static class DftDescriptor extends BasicFormatDescriptor {
        public DftDescriptor() {
            super();
        }


        @Override
        public EndType getEndType() {
            return EndType.SINK;
        }

        @Override
        protected void appendOptionCfgs(Options options) {
            options.add("ignoreParseErrors", TISFlinkProp.create(DebeziumJsonFormatOptions.IGNORE_PARSE_ERRORS));
            options.add("timestampFormat", TISFlinkProp.create(DebeziumJsonFormatOptions.TIMESTAMP_FORMAT));
//            options.add("nullKeyMode", TISFlinkProp.create(DebeziumJsonFormatOptions.JSON_MAP_NULL_KEY_MODE));
//            options.add("nullKeyLiteral", TISFlinkProp.create(DebeziumJsonFormatOptions.JSON_MAP_NULL_KEY_LITERAL));

            addNullKeyOptCfg(options);

            options.add("encodeDecimal", TISFlinkProp.create(JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER));
        }

        @Override
        public String getDisplayName() {
            return org.apache.flink.formats.json.debezium.DebeziumJsonFormatFactory.IDENTIFIER;
        }
    }
}
