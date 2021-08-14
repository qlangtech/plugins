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
package com.qlangtech.async.message.client.to.impl;

import com.qlangtech.tis.async.message.client.consumer.impl.AbstractAsyncMsgDeserialize;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.realtime.transfer.DTO;

import java.io.IOException;
import java.nio.charset.CharsetDecoder;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class DefaultJSONFormatDeserialize extends AbstractAsyncMsgDeserialize {
    // CharsetDecoder 有线程安全问题
    private static final ThreadLocal<CharsetDecoder> utf8CharsetDecoder = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return TisUTF8.get().newDecoder();
        }
    };

    @Override
    public final DTO deserialize(byte[] content) throws IOException {
        return com.alibaba.fastjson.JSONObject.parseObject(content, 0, content.length, utf8CharsetDecoder.get(), DTO.class);
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<AbstractAsyncMsgDeserialize> {

        @Override
        public String getDisplayName() {
            return "defaultJson";
        }
    }
}
