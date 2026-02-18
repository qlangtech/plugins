package com.qlangtech.tis.plugin.akka;

import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.plugin.datax.BasicWorkflowInstance;
import com.qlangtech.tis.plugin.datax.IWorkflowNode;

import java.util.List;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/31
 */
public class AkkaWorkflow extends BasicWorkflowInstance {
    public AkkaWorkflow(ExecutePhaseRange executePhaseRange, String appName, Long spiWorkflowId) {
        super(executePhaseRange, appName, spiWorkflowId);
    }

    @Override
    public boolean isDisabled() {
        return false;
    }

    @Override
    public List<IWorkflowNode> getWorkflowNodes() {
        return List.of();
    }
}
