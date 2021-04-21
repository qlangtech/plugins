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
