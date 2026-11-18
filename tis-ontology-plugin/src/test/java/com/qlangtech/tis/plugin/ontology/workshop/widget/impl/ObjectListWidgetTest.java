package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DefaultDescriptorsJSON;
import com.qlangtech.tis.util.DescriptorsMeta;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.PartialSettedPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.junit.Before;
import org.junit.Test;

import static com.qlangtech.tis.plugin.ontology.OntologyDomain.KEY_WORKSHOP;
import static com.qlangtech.tis.plugin.ontology.OntologyDomain.NAME_ONTOLOGY_DOMAIN;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/16
 */
public class ObjectListWidgetTest {
    @Before
    public void setUp() {
        PartialSettedPluginContext pluginContext = new PartialSettedPluginContext();
        DefaultContext context = new DefaultContext();
        // 级联管道读 UploadPluginMeta.createPluginMeta(context)；没有它就是 NPE（无降级分支）
        UploadPluginMeta.putPluginMeta(context, UploadPluginMeta.parse("widget:require," + NAME_ONTOLOGY_DOMAIN + "_test," + KEY_WORKSHOP + "_test"));
        pluginContext.setContext(context);
        IPluginContext.setPluginContext(pluginContext);

    }

    @Test
    public void testDescriptorsJSON() {
//        IPluginContext.setPluginContext(IPluginContext.namedContext("test_pipeline"));
//        UploadPluginMeta.createPluginMeta();
        DescriptorsMeta descriptorsJSON = new DefaultDescriptorsJSON(new ObjectListWidget.DescriptorImpl()).getDescriptorsJSON();
        System.out.println(JsonUtil.toString(descriptorsJSON, true));
    }
}