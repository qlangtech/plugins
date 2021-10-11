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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.extension.ExtensionList;
import junit.framework.TestCase;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 21:48
 **/
public class TestDataXJobSubmit extends TestCase {
    public void testTwoInstanceGetter() {
        ExtensionList<DataXJobSubmit> extensionList = TIS.get().getExtensionList(DataXJobSubmit.class);
        assertNotNull(extensionList);
        assertEquals(2, extensionList.size());
    }
}
