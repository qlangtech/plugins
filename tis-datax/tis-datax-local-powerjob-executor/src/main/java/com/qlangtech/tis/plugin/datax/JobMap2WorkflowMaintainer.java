package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/10
 */
public class JobMap2WorkflowMaintainer {
    private final Map<String, Long> jobName2JobId = Maps.newHashMap();
    private final Map<Long, WorkflowNodeInfoDTO> jobIdMap2Workflow = Maps.newHashMap();

    public void addJob(ISelectedTab selectedTab, Long jobId) {
        Objects.requireNonNull(jobId, "jobId can not be null");
        this.jobName2JobId.put(selectedTab.getName(), jobId);
    }

    public void addWorkflow(List<WorkflowNodeInfoDTO> savedWfNodes) {
        Objects.requireNonNull(savedWfNodes, "savedWfNodes can not be null");
        for (WorkflowNodeInfoDTO wf : savedWfNodes) {
            jobIdMap2Workflow.put(wf.getJobId(), wf);
        }
    }

    public PEWorkflowDAG createWorkflowDAG() {
// DAG 图
        List<PEWorkflowDAG.Node> nodes = Lists.newLinkedList();
        List<PEWorkflowDAG.Edge> edges = Lists.newLinkedList();
        for (WorkflowNodeInfoDTO wf : jobIdMap2Workflow.values()) {
            nodes.add(new PEWorkflowDAG.Node(wf.getId()));
        }

//        dagSession.buildSpec((dpt) -> {
//            edges.add(new PEWorkflowDAG.Edge(getWfIdByJobName(dpt.getLeft())
//                    , getWfIdByJobName(dpt.getRight())));
//        });

        return new PEWorkflowDAG(nodes, edges);
    }

    private Long getWfIdByJobName(String jobName) {

        Long jobId = jobName2JobId.get(jobName);
        if (jobId == null) {
            throw new IllegalStateException("jobName:" + jobName + " relevant jobId can not be null,exist:"
                    + jobName2JobId.entrySet().stream().map((entry) -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(",")));
        }

        WorkflowNodeInfoDTO wfInfo = jobIdMap2Workflow.get(jobId);
        if (wfInfo == null) {
            throw new IllegalStateException("jobId:" + jobId + " relevant workflowInfo  can not be null,exist:"
                    + jobIdMap2Workflow.entrySet().stream().map((entry) -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(",")));
        }
        return wfInfo.getId();
    }
}
