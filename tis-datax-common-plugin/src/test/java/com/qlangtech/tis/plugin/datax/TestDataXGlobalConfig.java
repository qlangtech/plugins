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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;

import java.util.Collections;
import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-04-21 10:37
 **/
public class TestDataXGlobalConfig extends TestCase {

    public void testDescriptJSONGenerate() {

        DataXGlobalConfig globalConfig = new DataXGlobalConfig();
        Descriptor<ParamsConfig> descriptor = globalConfig.getDescriptor();
        assertNotNull(descriptor);

        List<Descriptor<ParamsConfig>> singleton = Collections.singletonList(descriptor);
        DescriptorsJSON descriptorsJSON = new DescriptorsJSON(singleton);

        JSON.parseObject(IOUtils.loadResourceFromClasspath(TestDataXGlobalConfig.class, "dataXGlobalConfig-descriptor-assert.json"));

       // System.out.println(descriptorsJSON.getDescriptorsJSON().toJSONString());

        assertEquals(JSON.parseObject(IOUtils.loadResourceFromClasspath(TestDataXGlobalConfig.class, "dataXGlobalConfig-descriptor-assert.json")).toJSONString()
                , descriptorsJSON.getDescriptorsJSON().toJSONString());

    }
}
