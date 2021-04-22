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
package com.qlangtech.async.message.client.consumer.support;

import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;

/*
 * 消费类接口,实现该类处理具体的业务类型
 * <p>例子: DoBusiness类只处理messageTagA和messageTagB的消息</p>
 * <blockquote><pre>
 * @MessageTag(tag = {"messageTagA","messageTagB"})
 * public class DoBusiness implements ConsumerCallBack {
 *      @Override
 *      public boolean process(AsyncMsgTO msgTO) {
 *          //do business
 *          return false;
 *      }
 * }
 * </pre></blockquote>
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015-12-09
 */
public interface ConsumerCallBack {

    /**
     * 业务具体处理方法
     *
     * @return
     */
    boolean process(AsyncMsg msg);
}
