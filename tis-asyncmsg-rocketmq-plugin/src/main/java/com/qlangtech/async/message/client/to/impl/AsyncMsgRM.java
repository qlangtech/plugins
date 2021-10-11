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
package com.qlangtech.async.message.client.to.impl;

import com.qlangtech.async.message.client.util.MsgUtils;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/*
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class AsyncMsgRM implements AsyncMsg {

    private static final long serialVersionUID = 1L;

    private Logger log = LoggerFactory.getLogger(AsyncMsgRM.class);

    private MessageExt messageExt;

    private final IAsyncMsgDeserialize<DTO> deserialize;

    public AsyncMsgRM(MessageExt messageExt, IAsyncMsgDeserialize deserialize) {
        this.messageExt = messageExt;
        if (deserialize == null) {
            throw new IllegalArgumentException("param deseriablize can not be null");
        }
        this.deserialize = deserialize;
    }

    @Override
    public Object getSource() throws IOException {
        return null;
    }


    /**
     * 取消息体，已经反序列化
     *
     * @return
     */
    public DTO getContent() throws IOException {
        return this.deserialize.deserialize(messageExt.getBody());
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }

    @Override
    public Set<String> getFocusTabs() {
        return null;
    }

    @Override
    public String getTopic() {
        return messageExt.getTopic();
    }

    @Override
    public String getTag() {
        return messageExt.getTags();
    }

    // @Override
    public String getKey() {
        return messageExt.getKeys();
    }

    /**
     * 取消息id
     *
     * @return
     */
    @Override
    public String getMsgID() {
        return messageExt.getMsgId();
    }

    /**
     * 取重试次数
     *
     * @return
     */
    //  @Override
    public int getReconsumeTimes() {
        return messageExt.getReconsumeTimes();
    }

    /**
     * 取开始投递的时间
     *
     * @return
     */
    //  @Override
    public long getStartDeliverTime() {
        return messageExt.getBornTimestamp();
    }

    //  @Override
    public String getOriginMsgID() {
        return MsgUtils.getOriginMsgId(messageExt);
    }

//    @Override
//    public MessageExt getMessage() {
//        return messageExt;
//    }

    public String toString() {
        return messageExt.toString();
    }
}
