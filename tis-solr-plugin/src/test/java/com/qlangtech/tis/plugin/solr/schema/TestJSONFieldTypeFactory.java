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
