package com.qlangtech.async.message.client.kafka;

import com.google.common.collect.Maps;
import com.pingcap.ticdc.cdc.TicdcEventData;
import com.pingcap.ticdc.cdc.key.TicdcEventKey;
import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import com.pingcap.ticdc.cdc.value.TicdcEventRowChange;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.realtime.transfer.DTO;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author: baisui 百岁
 * @create: 2020-12-09 14:33
 **/
public class KafkaAsyncMsg implements AsyncMsg {
    // private final TicdcEventData data;
    // 为了让DefaultTable的validateTable方法通过，这里需要添加一个占位符，其实没有什么用的
    private static final Map<String, String> beforeValues = Collections.singletonMap("tis_placeholder", "1");


    private final String tableName;
    private final String topicName;
    private final TicdcEventKey ticdcEventKey;
    final boolean update;
    private TicdcEventRowChange value;

    public KafkaAsyncMsg(String topicName, TicdcEventData data) {
        this.topicName = topicName;
        this.ticdcEventKey = data.getTicdcEventKey();
        ticdcEventKey.getTimestamp();
        this.tableName = ticdcEventKey.getTbl();
        value = (TicdcEventRowChange) data.getTicdcEventValue();
        this.update = "u".equals(value.getUpdateOrDelete());
    }

    @Override
    public String getTopic() {
        return this.topicName;
    }

    @Override
    public String getTag() {
        return this.tableName;
    }

    @Override
    public DTO getContent() throws IOException {
        DTO dto = new DTO();
        dto.setDbName(this.ticdcEventKey.getScm());
        dto.setEventType(this.update ? DTO.EventType.UPDATE.getTypeName() : DTO.EventType.ADD.getTypeName());
        dto.setOrginTableName(this.tableName);
        Map<String, String> after = Maps.newHashMap();
        for (TicdcEventColumn col : this.value.getColumns()) {
            after.put(col.getName(), String.valueOf(col.getV()));
        }
        dto.setAfter(after);
        dto.setBefore(beforeValues);
        return dto;
    }

    @Override
    public String getMsgID() {
        return null;
    }

}
