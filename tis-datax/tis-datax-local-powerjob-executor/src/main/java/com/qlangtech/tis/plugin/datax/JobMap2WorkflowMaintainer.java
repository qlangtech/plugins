package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.sql.parser.ISqlTask;
import org.apache.commons.lang.StringUtils;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/10
 */
public class JobMap2WorkflowMaintainer {
    private final Map<String, Long> jobName2JobId = Maps.newHashMap();
    private final Map<Long, WorkflowNodeInfoDTO> jobIdMap2Workflow = Maps.newHashMap();

    /**
     * 初始启动节点id，用于在定时任务执行时，取得TaskId
     */
    private WorkflowNodeInfoDTO startInitNode;

    public void addJob(ISelectedTab selectedTab, Long jobId) {
        Objects.requireNonNull(jobId, "jobId can not be null");
        this.jobName2JobId.put(selectedTab.getName(), jobId);
    }

    /**
     * 添加分析节点
     *
     * @param processTask
     * @param jobId
     */
    public void addJob(ISqlTask processTask, Long jobId) {
        Objects.requireNonNull(jobId, "jobId can not be null");
        this.jobName2JobId.put(processTask.getExportName(), jobId);
    }

    public void addWorkflow(List<WorkflowNodeInfoDTO> savedWfNodes) {
        Objects.requireNonNull(savedWfNodes, "savedWfNodes can not be null");
        for (WorkflowNodeInfoDTO wf : savedWfNodes) {
            jobIdMap2Workflow.put(wf.getJobId(), wf);
        }
    }

    public void setStartInitJob(Optional<WorkflowNodeInfoDTO> startInitNode) {
        this.startInitNode = startInitNode.orElseThrow(() -> new IllegalStateException("startInitNode can not be null"));
    }

    public final PEWorkflowDAG createWorkflowDAG() {
// DAG 图
        List<PEWorkflowDAG.Node> nodes = Lists.newLinkedList();
        List<PEWorkflowDAG.Edge> edges = Lists.newLinkedList();

        Long startInitNodeId = Objects.requireNonNull(this.startInitNode, "startInitNode can not be null").getId();

        for (WorkflowNodeInfoDTO wf : jobIdMap2Workflow.values()) {
            nodes.add(new PEWorkflowDAG.Node(wf.getId()));
            // 开始节点到每天数据同步节点都有一条边
            if (!startInitNodeId.equals(wf.getId())) {
                edges.add(new PEWorkflowDAG.Edge(startInitNodeId, wf.getId()));
            }
        }


        return createWorkflowDAG(nodes, edges);
    }

    protected PEWorkflowDAG createWorkflowDAG(List<PEWorkflowDAG.Node> nodes, List<PEWorkflowDAG.Edge> edges) {
        //  dagSession.buildSpec((dpt) -> {
//            edges.add(new PEWorkflowDAG.Edge(getWfIdByJobName(dpt.getLeft())
//                    , getWfIdByJobName(dpt.getRight())));
//        });
        return new PEWorkflowDAG(nodes, edges);
    }

    /**
     * powerjob workflowId
     *
     * @param jobName
     * @return
     */
    protected final Long getWfIdByJobName(String jobName) {
        if (StringUtils.isEmpty(jobName)) {
            throw new IllegalArgumentException("param jobName can not be null");
        }
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


    public void beforeCreateWorkflowDAG(K8SDataXPowerJobJobTemplate jobTpl) {

    }
}
