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
package com.qlangtech.async.message.client.mqttSender;

/*
 * Created with IntelliJ IDEA.
 * User:jiandan
 * Date:2015/6/19.
 * Time:13:23.
 * INFO:用于向 Mqtt 客户端发送消息
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public interface IMqttSendManager {

    /**
     * 发消息，此时发送key取md5(msgTopic+msgTag+内容)，默认采用 FastJson 序列化
     *
     * @param msg  消息内容
     * @param otherParams
     * @return 序列id，出错时返回null
     */
    Boolean sendMsg(String msg, String... otherParams);

    /**
     * 发送P2P 消息
     * @param msg
     * @param deviceIds
     */
    void sendP2pMsg(String msg, String... deviceIds);

    /**
     * 重新初始化MQTT连接池
     */
    void reConnection();
}
