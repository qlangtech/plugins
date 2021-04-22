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
package com.qlangtech.async.message.client.util;

import org.apache.rocketmq.common.message.MessageExt;

/*
 * @Date 2017/3/21
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class MsgUtils {

    public static final String MESSAGE_TRACE = "message_trace";

    public static final <T> boolean isAllNull(T... objs) {
        for (T t : objs) {
            if (t != null)
                return false;
        }
        return true;
    }

    public static String getOriginMsgId(MessageExt messageExt) {
        if (messageExt.getReconsumeTimes() > 0) {
            String msgId = messageExt.getProperty("ORIGIN_MESSAGE_ID");
            // }
            return msgId;
        }
        return messageExt.getMsgId();
    }
}
