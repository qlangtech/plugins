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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.canaljson;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.TISFlinkProp;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.canal.CanalJsonFormatFactory;
import org.apache.flink.formats.json.canal.CanalJsonFormatOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/formats/canal/
 * example:
 * <pre>
 *     {
 * 	"data": [{
 * 		"order_id": "99933194ffe98b9e407c4b61a67962da",
 * 		"global_code": "201711241047526500101263",
 * 		"simple_code": "6500101291",
 * 		"seat_code": "110",
 * 		"code": 1,
 * 		"curr_date": "2024-10-10",
 * 		"totalpay_id": "38518a1547e449bdb1be9ecc7ec1f2dd",
 * 		"seat_id": "999331945e835e4a015e845261e10037",
 * 		"people_count": 4,
 * 		"open_time": 1511756903416,
 * 		"status": 2,
 * 		"memo": "",
 * 		"inner_code": "201711240001",
 * 		"menutime_id": "",
 * 		"worker_id": "f703b0c370774d6aacf98ba305c268a0",
 * 		"end_time": 0,
 * 		"feeplan_id": "",
 * 		"op_user_id": "f703b0c370774d6aacf98ba305c268a0",
 * 		"order_from": 0,
 * 		"order_kind": 1,
 * 		"area_id": "e77f04c1f10241c79f8b7ee22e657896",
 * 		"name": "",
 * 		"mobile": "",
 * 		"tel": "",
 * 		"is_autocommit": 0,
 * 		"send_time": 1511491936988,
 * 		"address": "",
 * 		"paymode": 0,
 * 		"outfee": 0.56,
 * 		"sender_id": "",
 * 		"customerregister_id": "",
 * 		"waitingorder_id": "",
 * 		"send_status": 0,
 * 		"audit_status": 0,
 * 		"is_hide": 0,
 * 		"entity_id": "99933194",
 * 		"is_valid": 0,
 * 		"create_time": 1511756903416,
 * 		"op_time": 1511776094273,
 * 		"last_ver": 65,
 * 		"load_time": 1511756903,
 * 		"modify_time": 1511776130,
 * 		"is_limittime": 1,
 * 		"scan_url": "http://api.l.whereask.com/ma/order/201711241047526500101263",
 * 		"seat_mark": "",
 * 		"reservetime_id": "",
 * 		"is_wait": 0,
 * 		"is_print": 1,
 * 		"book_id": "",
 * 		"reserve_id": "",
 * 		"orign_id": "",
 * 		"reserve_status": 0,
 * 		"ext": ""
 *        }],
 * 	"type": "INSERT",
 * 	"table": "orderdetail",
 * 	"ts": 1735375140702
 * }
 *
 * </pre>
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-15 12:42
 **/
public class TISCanalJsonFormatFactory extends FormatFactory {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, advance = true)
    public Boolean ignoreParseErrors;
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, advance = true, validate = Validator.require)
    public String timestampFormat;
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, advance = true)
    public String dbInclude;
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, advance = true)
    public String tableInclude;
    @FormField(ordinal = 4, type = FormFieldType.ENUM, advance = true)
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
    public String getNullFormat() {
        return this.nullKeyLiteral;
    }

    @Override
    protected String getTimestampFormat() {
        return this.timestampFormat;
    }

    private static final String FIELD_OLD = "old";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_DELETE = "DELETE";
    private static final String OP_CREATE = "CREATE";

    @Override
    public KafkaStructuredRecord parseRecord(KafkaStructuredRecord reuse, byte[] record) {
        HashMap jsonObject = JSON.parseObject(record, HashMap.class);
        List<Map> data = (List<Map>) jsonObject.get("data");
        if (data == null) {
            return null;
        }
        String eventType = (String) jsonObject.get("type");
        reuse.setOldVals(null);
        reuse.setVals(null);
        switch (eventType) {
            case OP_INSERT:
            case OP_CREATE:
                reuse.setEventType(EventType.ADD);
                break;
            case OP_UPDATE:
                reuse.setEventType(EventType.UPDATE_AFTER);
                break;
            case OP_DELETE:
                reuse.setEventType(EventType.DELETE);
                break;
            default:
                return null;
        }
        reuse.setTabName((String) jsonObject.get("table"));

        if (data != null) {
            for (Map d : data) {
                reuse.setVals(d);
                break;
            }
        }
        List<Map> olds = (List<Map>) jsonObject.get(FIELD_OLD);
        if (olds != null) {
            for (Map before : olds) {
                reuse.setOldVals(before);
                break;
            }
        }

        return reuse;
    }

    @Override
    public boolean acceptMultipleTable() {
        return true;
    }

//    @Override
//    public List<String> parseTargetTabsEntities() {
//        throw new UnsupportedOperationException();
//    }

//    /**
//     * @return
//     * @see org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema#deserialize(byte[], org.apache.flink.util.Collector <RowData> )
//     */
//    @Override
//    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(final String targetTabName) {
//        return createFormat(targetTabName, (factory, cfg) -> {
//            return factory.createDecodingFormat(null, cfg);
//        });
//    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(final String targetTabName) {
        return createFormat(targetTabName, (factory, cfg) -> {
            return factory.createEncodingFormat(null, cfg);
        });
    }

    private <T> T createFormat(String targetTabName
            , BiFunction<CanalJsonFormatFactory, org.apache.flink.configuration.Configuration, T> formatCreator) {
        CanalJsonFormatFactory canalFormatFactory = new CanalJsonFormatFactory();

        DftDescriptor desc = (DftDescriptor) this.getDescriptor();
        return formatCreator.apply(canalFormatFactory
                , desc.options.createFlinkCfg(this)
                        .set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));

//        return canalFormatFactory.createEncodingFormat(null
//                , desc.options.createFlinkCfg(this).set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));
    }

//    public static class DefaultDescriptor extends FlinkDescriptor<CompactionConfig> {
//        public DefaultDescriptor() {
//            addFieldDescriptor("payloadClass", FlinkOptions.PAYLOAD_CLASS_NAME);
//            addFieldDescriptor("targetIOPerInMB", FlinkOptions.COMPACTION_TARGET_IO);
//            addFieldDescriptor("triggerStrategy", FlinkOptions.COMPACTION_TRIGGER_STRATEGY);
//            addFieldDescriptor("maxNumDeltaCommitsBefore", FlinkOptions.COMPACTION_DELTA_COMMITS);
//            addFieldDescriptor("maxDeltaSecondsBefore", FlinkOptions.COMPACTION_DELTA_SECONDS);
//            addFieldDescriptor("asyncClean", FlinkOptions.CLEAN_ASYNC_ENABLED);
//            addFieldDescriptor("retainCommits", FlinkOptions.CLEAN_RETAIN_COMMITS);
//            addFieldDescriptor(KEY_archiveMinCommits, FlinkOptions.ARCHIVE_MIN_COMMITS);
//            addFieldDescriptor(KEY_archiveMaxCommits, FlinkOptions.ARCHIVE_MAX_COMMITS);
//        }


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
            options.add("ignoreParseErrors", TISFlinkProp.create(CanalJsonFormatOptions.IGNORE_PARSE_ERRORS));
            options.add("timestampFormat", TISFlinkProp.create(CanalJsonFormatOptions.TIMESTAMP_FORMAT));
            options.add("dbInclude", TISFlinkProp.create(CanalJsonFormatOptions.DATABASE_INCLUDE));
            options.add("tableInclude", TISFlinkProp.create(CanalJsonFormatOptions.TABLE_INCLUDE));
            addNullKeyOptCfg(options);
            options.add("encodeDecimal", TISFlinkProp.create(JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER));
        }


        @Override
        public String getDisplayName() {
            return CanalJsonFormatFactory.IDENTIFIER;
        }
    }

    public static void main(String[] args) {

    }

}



