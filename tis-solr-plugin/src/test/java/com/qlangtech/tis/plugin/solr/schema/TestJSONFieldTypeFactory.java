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

package com.qlangtech.tis.plugin.solr.schema;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-02-06 10:41
 **/
public class TestJSONFieldTypeFactory extends TestCase {
    /**
     * 表单生成
     */
    public void testFormCreate() throws Exception {
        List<Descriptor<FieldTypeFactory>> descriptors = TIS.get().getDescriptorList(FieldTypeFactory.class);
        assertNotNull(descriptors);
        assertEquals(1, descriptors.size());
        DescriptorsJSON desc2Json = new DescriptorsJSON(descriptors);
        String assertFile = "fieldTypeFactory-descriptor-assert.json";
        try (InputStream reader = this.getClass().getResourceAsStream(assertFile)) {
            assertNotNull(reader);
            // FileUtils.write(new File(assertFile), JsonUtil.toString(desc2Json.getDescriptorsJSON()), TisUTF8.get(), false);
            assertEquals(IOUtils.toString(reader, TisUTF8.get()), JsonUtil.toString(desc2Json.getDescriptorsJSON()));
        }
    }
}
