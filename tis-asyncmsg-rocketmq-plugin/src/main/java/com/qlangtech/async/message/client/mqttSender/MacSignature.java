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

import org.apache.commons.codec.binary.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * Created by heytong on 15/6/9.
 * INFO:MQTT 消息签名
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class MacSignature {

    /**
     * 发送⽅方签名⽅方法
     *
     * @param text  Mqtt ClientId
     * @param secretKey 阿⾥里云ONS secretKey
     * @return 加密后的字符串
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.security.InvalidKeyException
     */
    public static String macSignature(String text, String secretKey) throws InvalidKeyException, NoSuchAlgorithmException {
        Charset charset = Charset.forName("UTF-8");
        String algorithm = "HmacSHA1";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(secretKey.getBytes(charset), algorithm));
        byte[] bytes = mac.doFinal(text.getBytes(charset));
        return new String(Base64.encodeBase64(bytes), charset);
    }

    public static String publishSignature(String clientId, String secretKey) throws NoSuchAlgorithmException, InvalidKeyException {
        return macSignature(clientId, secretKey);
    }

    /**
     * 订阅⽅方签名⽅方法
     *
     * @param topics 要订阅的Topic集合
     * @param clientId  Mqtt ClientId
     * @param secretKey 阿⾥里云ONS secretKey
     * @return 加密后的字符串
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.security.InvalidKeyException
     */
    public static String subSignature(List<String> topics, String clientId, String secretKey) throws NoSuchAlgorithmException, InvalidKeyException {
        // 以字典顺序排序
        Collections.sort(topics);
        String topicText = "";
        for (String topic : topics) {
            topicText += topic + "\n";
        }
        String text = topicText + clientId;
        return macSignature(text, secretKey);
    }

    /**
     * 订阅⽅方签名⽅方法
     *
     * @param topic 要订阅的Topic
     * @param clientId  Mqtt ClientId
     * @param secretKey 阿⾥里云ONS secretKey
     * @return 加密后的字符串
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.security.InvalidKeyException
     */
    public static String subSignature(String topic, String clientId, String secretKey) throws NoSuchAlgorithmException, InvalidKeyException {
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        return subSignature(topics, clientId, secretKey);
    }
}
