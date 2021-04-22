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
package com.qlangtech.tis.util;

import com.google.common.collect.Lists;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import junit.framework.TestCase;

import java.util.List;

/*
 * @create: 2020-02-08 16:23
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestHeteroList extends TestCase {

    private static final String caption = "test-caption";

    public void testReflectAllMethod() {
        // HeteroList<AbstractMQListenerFactory> hList = new HeteroList<>();
        List<MQListenerFactory> items = Lists.newArrayList();
        // 
        // final Type col = Types.getBaseClass(items.getClass(), List.class);
        // System.out.println(col);
        // System.out.println(items.getClass().getComponentType());
        HeteroList<MQListenerFactory> heteroList = HeteroList.getHeteroList(caption, items, MQListenerFactory.class);
        assertEquals(caption, heteroList.getCaption());
        // assertEquals(1, heteroList.getDescriptors().size());
        assertEquals(0, heteroList.getItems().size());
    }
}
