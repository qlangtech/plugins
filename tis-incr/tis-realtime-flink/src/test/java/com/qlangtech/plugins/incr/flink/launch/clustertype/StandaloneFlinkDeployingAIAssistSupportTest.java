package com.qlangtech.plugins.incr.flink.launch.clustertype;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.qlangtech.tis.aiagent.core.IAgentContext;
import com.qlangtech.tis.util.IPluginContext;
import org.junit.Test;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/8
 */
public class StandaloneFlinkDeployingAIAssistSupportTest {
    @Test
    public void testStartProcess() throws Exception {
        StandaloneFlinkDeployingAIAssistSupport assistSupport = new StandaloneFlinkDeployingAIAssistSupport();
        assistSupport.port = 8081;
        assistSupport.slot = 1;
        assistSupport.tmMemory = com.qlangtech.tis.plugin.MemorySize.ofMebiBytes(1728);
        assistSupport.flinkDeployDir = "/opt/misc";
        assistSupport.dataDir = "/opt/data/tis";
        IPluginContext pluginContext = IPluginContext.namedContext("xxx");
        Context ctx = new DefaultContext();

        assistSupport.startProcess(IAgentContext.createNull(), pluginContext, ctx);
    }
}
