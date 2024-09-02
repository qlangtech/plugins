/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataXPowerJobSubmit;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.PowerWorkflowPayload.PowerJobWorkflow;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobWorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient;
import com.qlangtech.tis.plugin.datax.powerjob.impl.TISWorkflowInfoDTO;
import com.qlangtech.tis.plugin.datax.powerjob.impl.WorkflowListResult;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta.SqlDataFlowTopology;
import com.qlangtech.tis.sql.parser.meta.NodeType;
import com.qlangtech.tis.sql.parser.meta.NodeType.NodeTypeParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.model.PEWorkflowDAG.Node;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.request.http.SaveWorkflowNodeRequest;
import tech.powerjob.common.response.WorkflowInfoDTO;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient.result;

/**
 * 利用PowerJob触发任务
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 21:41
 **/
@TISExtension()
@Public
public class DistributedPowerJobDataXJobSubmit extends BasicDistributedSPIDataXJobSubmit<PowerJobWorkflow> implements IDataXPowerJobSubmit {


    private static final Logger logger = LoggerFactory.getLogger(DistributedPowerJobDataXJobSubmit.class);

    public DistributedPowerJobDataXJobSubmit() {
    }

    @Override
    protected PowerWorkflowPayload createWorkflowPayload(IControlMsgHandler module, SqlDataFlowTopology topology) {
        return PowerWorkflowPayload.createTISWorkflowPayload(this, module, topology);
    }

    @Override
    protected PowerWorkflowPayload createApplicationPayload(IControlMsgHandler module, String appName) {
        return PowerWorkflowPayload.createApplicationPayload(this, module, appName);
    }

    @Override
    public InstanceType getType() {
        return InstanceType.DISTRIBUTE;
    }

    @Override
    public boolean cancelTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {
        TISPowerJobClient tisPowerJob = getTISPowerJob();
        WorkFlowBuildHistoryPayload buildHistoryPayload
                = new PowerjobWorkFlowBuildHistoryPayload(null, buildHistory.getTaskId(), getCommonDAOContext(module), tisPowerJob);

        result(tisPowerJob.stopWorkflowInstance(buildHistoryPayload.getSPIWorkflowInstanceId()));
        buildHistoryPayload.updateFinalStatus(ExecResult.CANCEL);
        return true;
    }

    @Override
    public void deleteWorkflow(IControlMsgHandler module, Context context, Long wfId) {
        Objects.requireNonNull(wfId, "wfId can not be null");
        TISPowerJobClient tisPowerJob = getTISPowerJob();

        result(tisPowerJob.deleteWorkflow(wfId));
    }

    /**
     * 取得所有当前管理的实例
     * pager.getCurPage(), pager.getRowsPerPage()
     *
     * @param <T>
     * @return
     */
    @Override
    public <T> Pair<Integer, List<T>> fetchAllInstance(Map<String, Object> criteria, int page, int pageSize) {
        TISPowerJobClient tisPowerJob = getTISPowerJob();

        WorkflowListResult workflowResult = tisPowerJob.fetchAllWorkflow(page - 1, pageSize);

        return Pair.of(workflowResult.getTotalItems()
                , workflowResult.getData().stream().map((r) -> (T) r).collect(Collectors.toList()));
    }


    public static void vistWorkflowNodes(String appName, WorkflowInfoDTO wfInfo, WorkflowNodeVisit visitor) {
        BasicWorkflowInstance.vistWorkflowNodes(appName, convertWorkflowNodes(wfInfo), visitor);
    }

    public static List<IWorkflowNode> convertWorkflowNodes(WorkflowInfoDTO wfInfo) {
        PEWorkflowDAG wfDAG = wfInfo.getPEWorkflowDAG();
        List<IWorkflowNode> nodes = Lists.newArrayList();
        for (PEWorkflowDAG.Node node : wfDAG.getNodes()) {
            nodes.add(new PowerJobWorkerNode(node));
        }
        return nodes;
    }

    private static class PowerJobWorkerNode implements IWorkflowNode {
        private final PEWorkflowDAG.Node node;

        public PowerJobWorkerNode(Node node) {
            this.node = node;
        }

        @Override
        public NodeType getNodeType() {
            try {

                if (this.parseSqlTaskCfg().isPresent()) {
                    return NodeType.JOINER_SQL;
                }

            } catch (NodeTypeParseException e) {
                throw new RuntimeException(e);
            }

            if (StringUtils.endsWith(node.getNodeName(), BasicDistributedSPIDataXJobSubmit.KEY_START_INITIALIZE_SUFFIX)) {
                return NodeType.START;
            }

            return NodeType.DUMP;

        }

        @Override
        public Long getNodeId() {
            return node.getNodeId();
        }

        @Override
        public Long getJobId() {
            return this.node.getJobId();
        }

        @Override
        public boolean getEnable() {
            return node.getEnable();
        }

        @Override
        public String getNodeName() {
            return node.getNodeName();
        }

        @Override
        public String getNodeParams() {
            return node.getNodeParams();
        }
    }

    /**
     * https://github.com/datavane/tis/issues/157
     *
     * @param module
     * @param context
     * @param dataxProcessor
     */
    @Override
    public void createJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(
                this, module, dataxProcessor.identityValue());

        appPayload.innerCreatePowerjobWorkflow(false, Optional.empty(), Optional.empty());
    }

    /**
     * @param module
     * @param context
     * @param topology
     */
    @Override
    public void createWorkflowJob(IControlMsgHandler module, Context context, IDataFlowTopology topology) {

        PowerWorkflowPayload payload = PowerWorkflowPayload.createTISWorkflowPayload(this, module, topology);

        payload.innerCreatePowerjobWorkflow(false,
                Optional.empty() //
                , Optional.empty() //
        );
    }


    public static TISPowerJobClient getTISPowerJob() {

        DataXJobWorker jobWorker = DataXJobWorker.getJobWorker(TargetResName.K8S_DATAX_INSTANCE_NAME);
        if (!(jobWorker instanceof ITISPowerJob)) {
            throw new IllegalStateException("jobWorker:"
                    + jobWorker.getClass().getName() + " must be type of :" + ITISPowerJob.class);
        }

        ITISPowerJob powerJob = (ITISPowerJob) jobWorker;
        TISPowerJobClient powerJobClient = powerJob.getPowerJobClient();
        return powerJobClient;
    }


    @Override
    public TISWorkflowInfoDTO saveJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(
                this, module, dataxProcessor.identityValue());


        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId = appPayload.saveJob();

        return (TISWorkflowInfoDTO) result(getTISPowerJob().fetchWorkflow(powerJobWorkflowId.getSPIWorkflowId()));
    }


    public static SaveWorkflowNodeRequest createWorkflowNode(PowerJobClient powerJobClient, String name, String jobParams
            , Optional<IWorkflowNode> existWfNode, SaveJobInfoRequest jobRequest, K8SDataXPowerJobJobTemplate jobTpl) {
        return createWorkflowNode(powerJobClient, name, jobParams, Optional.of(jobParams), existWfNode, jobRequest, jobTpl);
    }

    public static SaveWorkflowNodeRequest createWorkflowNode(
            PowerJobClient powerJobClient, String name, String jobParams, Optional<String> nodeParams
            , Optional<IWorkflowNode> existWfNode, SaveJobInfoRequest jobRequest, K8SDataXPowerJobJobTemplate jobTpl) {

        existWfNode.ifPresent((node) -> {
            jobRequest.setId(node.getJobId());
        });

        jobRequest.setJobName(name);
        jobRequest.setJobParams(jobParams);

        Long jobId = result(powerJobClient.saveJob(jobRequest));

        SaveWorkflowNodeRequest wfNode = jobTpl.createWorkflowNode();
        existWfNode.ifPresent((node) -> {
            wfNode.setId(node.getNodeId());
        });
        wfNode.setJobId(jobId);
        wfNode.setNodeName(name);
        wfNode.setNodeParams(nodeParams.get());
        return wfNode;
    }


}
