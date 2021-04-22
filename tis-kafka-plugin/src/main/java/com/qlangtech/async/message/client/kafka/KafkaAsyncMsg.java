/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
import java.util.List;
import java.util.Map;

/**
 * @author: baisui 百岁
 * @create: 2020-12-09 14:33
 **/
public class KafkaAsyncMsg implements AsyncMsg {
    // private final TicdcEventData data;
    // 为了让DefaultTable的validateTable方法通过，这里需要添加一个占位符，其实没有什么用的
   // private static final Map<String, String> beforeValues = Collections.singletonMap("tis_placeholder", "1");


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
        Map<String, String> before = Maps.newHashMap();
        for (TicdcEventColumn col : this.value.getColumns()) {
            after.put(col.getName(), String.valueOf(col.getV()));
        }
        dto.setAfter(after);
        List<TicdcEventColumn> oldCols = value.getOldColumns();
        if (oldCols != null) {
            for (TicdcEventColumn col : oldCols) {
                before.put(col.getName(), String.valueOf(col.getV()));
            }
        }

        dto.setBefore(before);
        return dto;
    }

    @Override
    public String getMsgID() {
        return null;
    }

}
