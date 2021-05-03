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

import junit.framework.TestCase;

import java.util.regex.Matcher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 09:37
 **/
public class TestK8SDataXJobWorker extends TestCase {
    public void testZKhostPattern() {
        //  K8SDataXJobWorker dataXJobWorker = new K8SDataXJobWorker();

        Matcher matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200:2181/tis/cloude");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200:2181");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200:2181/tis/cloude-1_bb");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200");
        assertFalse(matcher.matches());
    }
}
