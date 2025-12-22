package com.qlangtech.plugins.incr.flink.cdc.kingbase;

import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.util.IPluginContext;
import org.junit.Test;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/22
 */
public class FlinkCDCKingBaseSourceFactoryTest {

    @Test
    public void testDescGenerate() throws Exception {
        IPluginContext.setPluginContext(IPluginContext.namedContext("test_pipeline"));
        PluginDesc.testDescGenerate(FlinkCDCKingBaseSourceFactory.class
                , "flink-cdc-kingbase-source-factory-descriptor.json");
    }
}
