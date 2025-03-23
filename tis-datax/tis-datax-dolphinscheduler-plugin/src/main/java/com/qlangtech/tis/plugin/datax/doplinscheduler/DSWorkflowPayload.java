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

package com.qlangtech.tis.plugin.datax.doplinscheduler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.cloud.ICoordinator;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.PowerjobTriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.HttpUtils.PostParam;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshotUtils;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.BasicWorkflowInstance;
import com.qlangtech.tis.plugin.datax.BasicWorkflowPayload;
import com.qlangtech.tis.plugin.datax.IWorkflowNode;
import com.qlangtech.tis.plugin.datax.SPIExecContext;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DSTaskGroup.TaskGroup;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder.DolphinSchedulerResponse;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.ExportTISPipelineToDolphinscheduler;
import com.qlangtech.tis.plugin.datax.doplinscheduler.history.DSWorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.meta.NodeType;
import com.qlangtech.tis.sql.parser.meta.NodeType.NodeTypeParseException;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit.KEY_START_INITIALIZE_SUFFIX;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 18:04
 * // @see com.qlangtech.tis.plugin.datax.PowerWorkflowPayload
 **/
public class DSWorkflowPayload extends BasicWorkflowPayload<DSWorkflowInstance> {

    //    private static final long projectCdoe = 117442916207136l;
//    private static final String dsToken = "f02d2883118664579ea32a659b2c9652";
    private static final Logger logger = LoggerFactory.getLogger(DSWorkflowPayload.class);

    private static final String KEY_TASK_RESPONSE_DATA = "data";
    private static final String KEY_TASK_RESPONSE_SUCCESS = "success";


    private static final String KEY_TASK_DEFINITION_CODE = "code";
    private static final String KEY_TASK_DEFINITION_NAME = "name";
    private static final String KEY_TASK_PARAMS = "taskParams";
    private static final String kEY_TASK_LOCAL_PARAMS = "localParams";
    private static final String KEY_TASK_DESCRIPTION = "description";

    //           "taskGroupId": 1,
//                   "taskGroupPriority": 0,
    private static final String KEY_TASK_GROUP_ID = "taskGroupId";
    private static final String KEY_TASK_GROUP_PRIORITY = "taskGroupPriority";
    private static final String KEY_TASK_SOURCE_LOCATION_ARN = "sourceLocationArn";
    private static final String KEY_TASK_DESTINATION_LOCATION_ARN = "destinationLocationArn";

    private static final String KEY_RELATION_PRETASK_CODE = "preTaskCode";
    private static final String KEY_RELATION_PRETASK_VERSION = "preTaskVersion";
    private static final String KEY_RELATION_POSTTASK_CODE = "postTaskCode";


    private static final String KEY_LOCATION_TASK_CODE = "taskCode";
    private static final String KEY_LOCATION_X = "x";
    private static final String KEY_LOCATION_Y = "y";
    private final SPIWorkflowPayloadApplicationStore applicationStore;
    private final ExportTISPipelineToDolphinscheduler exportCfg;

    private final static AtomicInteger baseTisTaskIdWithLocalExecutor = new AtomicInteger(99999);

    public DSWorkflowPayload(ExportTISPipelineToDolphinscheduler exportCfg, IDataxProcessor dataxProcessor, ICommonDAOContext commonDAOContext, BasicDistributedSPIDataXJobSubmit submit) {
        super(dataxProcessor, commonDAOContext, submit);
        this.applicationStore = new SPIWorkflowPayloadApplicationStore(dataxProcessor.identityValue(), commonDAOContext);
        this.exportCfg = Objects.requireNonNull(exportCfg, "exportCfg can not be null");
    }

    @Override
    protected DSWorkflowInstance createWorkflowInstance(Long spiWorkflowId, ExecutePhaseRange execRange) {
        return new DSWorkflowInstance(this.exportCfg, execRange, this.dataxProcessor.identityValue(), spiWorkflowId);
    }

    @Override
    protected ReplicasSpec getResourceSeplicasSpec() {
       //  return ReplicasSpec.createDftPowerjobServerReplicasSpec();
        ExportTISPipelineToDolphinscheduler dsExport = Objects.requireNonNull(this.exportCfg, "exportCfg can not be null");
        return Objects.requireNonNull(dsExport.memorySpec, "memorySpec can not be null").getMemorySpec();
    }

    @Override
    protected DataXJobWorker getSPIJobWorker() {
        throw new UnsupportedOperationException("DolphinScheduler is get JobWorker is not support");
    }

    /**
     * 为了避免
     *
     * @see com.qlangtech.tis.datax.powerjob.CfgsSnapshotConsumer 中执行 synchronizTpisAndConfs方法processTaskIds由于
     * taskid不变导致停止本地文件更新本地配置，所以需要让 baseTisTaskIdWithLocalExecutor 参数每次来都往上递增
     */
    @Override
    public PowerjobTriggerBuildResult triggerWorkflow(Optional<Long> workflowInstanceIdOpt, RpcServiceReference feedback) {

        if (exportCfg.createHistory) {
            return super.triggerWorkflow(workflowInstanceIdOpt, feedback);
        } else {
            // 不需要在TIS端生成执行历史记录
            synchronized (baseTisTaskIdWithLocalExecutor) {
                baseTisTaskIdWithLocalExecutor.incrementAndGet();
                JSONObject instanceParams = createInstanceParams(baseTisTaskIdWithLocalExecutor.get());
                PowerjobTriggerBuildResult buildResult = new PowerjobTriggerBuildResult(true, instanceParams);
                buildResult.taskid = baseTisTaskIdWithLocalExecutor.get();
                return buildResult;
            }

        }
    }

    @Override
    protected String getPayloadContent() {
        return this.applicationStore.getPayloadContent();
    }

    @Override
    protected void setSPIWorkflowPayload(JSONObject appPayload) {
        this.applicationStore.setSPIWorkflowPayload(appPayload);
    }

    @Override
    public void innerCreatePowerjobWorkflow(boolean updateProcess,
                                            Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers
            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {
        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> pair = selectedTabTriggers.orElseGet(() -> createWfNodes());
        //  WorkflowUnEffectiveJudge unEffectiveJudge = unEffectiveOpt.orElseGet(() -> new WorkflowUnEffectiveJudge());
        /**===============================
         * 为执行任务添加task工作组
         ===============================*/
        TaskGroup taskGroup = Objects.requireNonNull(
                exportCfg.taskGroup, "exportCfg.taskGroup can not be null").addTaskGroup(exportCfg);
        try {
            this.innerSaveJob(taskGroup, updateProcess, this.dataxProcessor, pair.getKey());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean acceptTable(ISelectedTab selectedTab) {
        for (IdentityName tab : this.exportCfg.target.getTargetTables()) {
            if (StringUtils.equals(tab.identityValue(), selectedTab.getName())) {
                return true;
            }
        }
        return false;
    }

    //    @Override
//    protected DSWorkflowInstance loadWorkflowSPI() {
//        // ExecutePhaseRange executePhaseRange, String appName, Long spiWorkflowId
//        return new DSWorkflowInstance(, this.dataxProcessor.identityValue(), this);
//    }

    /**
     * http://192.168.28.201:12345/dolphinscheduler/projects/117442916207136/process-definition/117443067349472
     *
     * @param spiWorkflowId
     * @return
     */
    // @Override
    public static List<IWorkflowNode> getDAGNodes(ExportTISPipelineToDolphinscheduler exportCfg, BasicWorkflowInstance spiWorkflowId) {

        DolphinSchedulerResponse response = exportCfg.processDefinition().appendSubPath(spiWorkflowId.getSPIWorkflowId()).applyGet();
        if (!response.isSuccess()) {
            throw TisException.create(response.getMessage());
        }
        try {
            List<IWorkflowNode> nodes = Lists.newArrayList();
            JSONObject data = response.getData();
            JSONObject processDefinition = data.getJSONObject("processDefinition");

            final Long jobId = processDefinition.getLong("code");

            //JSONArray locations = JSONArray.parseArray(processDefinition.getString("locations"));

            //data.getJSONArray("processTaskRelationList");
            JSONArray tasksDefinition = data.getJSONArray("taskDefinitionList");

            JSONObject def = null;
            JSONObject taskParams = null;
            for (Object tskDef : tasksDefinition) {
                def = (JSONObject) tskDef;

                taskParams = def.getJSONObject("taskParams");
                Long nodeId = def.getLong("code");
                String nodeParams = taskParams.getString(KEY_TASK_SOURCE_LOCATION_ARN);

                NodeType nodeType = NodeType.parse(taskParams.getString(KEY_TASK_DESTINATION_LOCATION_ARN));
                // String nodeName, String nodeParams, Long jobId, Long nodeId
                nodes.add(new DSWorkflowNode(taskParams.getString("name"), nodeType, nodeParams, jobId, nodeId));
            }


            return nodes;
        } catch (NodeTypeParseException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    protected SPIExecContext createSPIExecContext() {
        SPIExecContext execContext = new SPIExecContext();
        execContext.setAppname(this.applicationStore.load().getProjectName());
        return execContext;
    }

    @Override
    protected WorkFlowBuildHistoryPayload createBuildHistoryPayload(Integer tisTaskId) {
        DSWorkFlowBuildHistoryPayload buildHistoryPayload = new DSWorkFlowBuildHistoryPayload(this.dataxProcessor, tisTaskId, this.commonDAOContext);
        return buildHistoryPayload;
    }

    @Override
    protected Long runSPIWorkflow(BasicWorkflowInstance spiWorkflowId, JSONObject instanceParams) {
        throw new UnsupportedOperationException("trigger by TIS is not support current");
    }


    private void innerSaveJob(
            TaskGroup taskGroup,
            boolean updateProcess,
            IDataxProcessor dataxProcessor
            , Map<ISelectedTab, SelectedTabTriggers> createWfNodesResult) throws Exception {

        // https://github.com/apache/dolphinscheduler/blob/dev/dolphinscheduler-api/src/test/java/org/apache/dolphinscheduler/api/controller/ProcessDefinitionControllerTest.java#L100
//        String relationJson =
//                "[{\"name\":\"\",\"pre_task_code\":0,\"pre_task_version\":0,\"post_task_code\":123456789,\"post_task_version\":1,"
//                        + "\"condition_type\":0,\"condition_params\":\"{}\"},{\"name\":\"\",\"pre_task_code\":123456789,\"pre_task_version\":1,"
//                        + "\"post_task_code\":123451234,\"post_task_version\":1,\"condition_type\":0,\"condition_params\":\"{}\"}]";
        // 新code获取办法
        // https://github.com/apache/dolphinscheduler/blob/57c80f2af5d1f29417fdbea09b27221f54987655/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java#L855
        SelectedTabTriggers tabTriggers = null;
        ISelectedTab selectedTab = null;
        JSONObject mrParams;
        boolean containPostTrigger = false;
        Optional<IWorkflowNode> changedWfNode = null;

        JSONArray definitionArray = new JSONArray();
        JSONArray relationArray = new JSONArray();
        JSONArray locationArray = new JSONArray();
        JSONObject taskParams = null;
        // CodeGenerateUtils codeGen = CodeGenerateUtils.genCode();

        int y_coordinate = 200;
        final int y_coordinate_step = 50;

        final long startTaskId = CodeGenerateUtils.genCode();
        JSONObject startNode = this.createTaskDefinitionJSON(taskGroup, startTaskId
                , "start " + dataxProcessor.identityValue()
                , "start pipeline of " + dataxProcessor.identityValue());
        taskParams = new JSONObject();

        taskParams.put(kEY_TASK_LOCAL_PARAMS
                , Lists.newArrayList(
                        createParam(JobParams.KEY_TASK_ID, ParamType.INTEGER)
                        , createParam(JobParams.KEY_COLLECTION, ParamType.VARCHAR)
                        , createParam(DataxUtils.EXEC_TIMESTAMP, ParamType.LONG)
                        , createParam(IFullBuildContext.DRY_RUN, ParamType.BOOLEAN)
                        , createParam(PluginAndCfgsSnapshotUtils.KEY_PLUGIN_CFGS_METAS, ParamType.VARCHAR)
                        , createParam(JobParams.KEY_JAVA_MEMORY_SPEC, ParamType.VARCHAR)
                        , createParam(JobParams.KEY_PREVIOUS_TASK_ID, ParamType.INTEGER)));
        JSONObject initNode = createInitNodeJson();
        this.setDisableGrpcRemoteServerConnect(initNode);
        //
        taskParams.put(KEY_TASK_SOURCE_LOCATION_ARN, JsonUtil.toString(initNode));
        taskParams.put(KEY_TASK_DESTINATION_LOCATION_ARN, NodeType.START.getType());
        taskParams.put(KEY_TASK_DEFINITION_NAME, dataxProcessor.identityValue() + KEY_START_INITIALIZE_SUFFIX);
        startNode.put(KEY_TASK_PARAMS, taskParams);

        definitionArray.add(startNode);
        relationArray.add(createTaskRelationJSON(-1, startTaskId));
        locationArray.add(createLocation(startTaskId, 200, y_coordinate + (y_coordinate_step * createWfNodesResult.size()) / 2));


        for (Map.Entry<ISelectedTab, SelectedTabTriggers> entry : createWfNodesResult.entrySet()) {

            final long taskCode = CodeGenerateUtils.genCode();

            //======================================
            tabTriggers = entry.getValue();
            selectedTab = entry.getKey();
            mrParams = new JSONObject(tabTriggers.createMRParams());
            this.setDisableGrpcRemoteServerConnect(mrParams);
            if (tabTriggers.getPostTrigger() != null) {
                containPostTrigger = true;
            }

//            SaveJobInfoRequest jobRequest = jobTpl.createSynJobRequest();
//            changedWfNode = unEffectiveJudge.getExistWfNode(selectedTab.getName());
//
//            SaveWorkflowNodeRequest wfNode = createWorkflowNode(this.getTISPowerJob(), dataxProcessor.identityValue() + "_" + selectedTab.getName()
//                    , JsonUtil.toString(mrParams), changedWfNode, jobRequest, jobTpl);
//            wfNodes.add(wfNode);
//            jobIdMaintainer.addJob(selectedTab, wfNode.getJobId());
            //======================================

            JSONObject taskDefinitionJson = createTaskDefinitionJSON(taskGroup,
                    taskCode
                    , selectedTab.getName() + "@" + dataxProcessor.identityValue()
                    , "execute pipeline of " + selectedTab.getName() + "@" + dataxProcessor.identityValue());
//            JsonUtil.loadJSON(DolphinschedulerExport.class, "tpl/task-definition-sync-tpl.json");
//            taskDefinitionJson.put(KEY_TASK_DEFINITION_CODE, taskCode);
//            taskDefinitionJson.put(KEY_TASK_DEFINITION_NAME, dataxProcessor.identityValue() + "@" + selectedTab.getName());
//            taskDefinitionJson.put(KEY_TASK_DESCRIPTION, "execute pipeline of " + dataxProcessor.identityValue() + "@" + selectedTab.getName());

            taskParams = new JSONObject();
            //  taskParams.put(kEY_TASK_LOCAL_PARAMS, Lists.newArrayList(createParam(JobParams.KEY_TASK_ID, true)));
            /**
             * 需要设置一个空值，不然会报以下异常
             * <pre>
             *     [ERROR] 2024-09-06 11:03:42.659 +0800 - Task execute failed, due to meet an exception
             * java.lang.NullPointerException: null
             * 	at org.apache.dolphinscheduler.server.worker.utils.TaskFilesTransferUtils.getFileLocalParams(TaskFilesTransferUtils.java:216)
             * 	at org.apache.dolphinscheduler.server.worker.utils.TaskFilesTransferUtils.downloadUpstreamFiles(TaskFilesTransferUtils.java:142)
             * 	at org.apache.dolphinscheduler.server.worker.runner.WorkerTaskExecutor.beforeExecute(WorkerTaskExecutor.java:231)
             * 	at org.apache.dolphinscheduler.server.worker.runner.WorkerTaskExecutor.run(WorkerTaskExecutor.java:164)
             * 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
             * 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
             * 	at java.lang.Thread.run(Thread.java:748)
             * </pre>
             */
            taskParams.put(kEY_TASK_LOCAL_PARAMS, new JSONArray());
            taskParams.put(KEY_TASK_SOURCE_LOCATION_ARN, JsonUtil.toString(mrParams));
            taskParams.put(KEY_TASK_DESTINATION_LOCATION_ARN, NodeType.DUMP.getType());
            taskParams.put(KEY_TASK_DEFINITION_NAME, selectedTab.getName() + "@" + dataxProcessor.identityValue());
            taskDefinitionJson.put(KEY_TASK_PARAMS, taskParams);


            definitionArray.add(taskDefinitionJson);

            relationArray.add(createTaskRelationJSON(startTaskId, taskCode));

            locationArray.add(createLocation(taskCode, 600, y_coordinate));
            y_coordinate += y_coordinate_step;
        }


        //System.out.println(taskParams);

        //  URL url = new URL(this.exportCfg.processDefinition().toString());
        List<PostParam> params = Lists.newArrayList();
        // params.add(new PostParam("projectCode", 117370558003456l));
        params.add(new PostParam("name", this.exportCfg.processName));
        params.add(new PostParam("description", this.exportCfg.processDescription));
        params.add(new PostParam("globalParams", "[]"));
        params.add(new PostParam("locations", JsonUtil.toString(locationArray)));
        params.add(new PostParam("taskRelationJson", JsonUtil.toString(relationArray)));

        String taskDefinition = JsonUtil.toString(definitionArray);
        //System.out.println(taskDefinition);
        params.add(new PostParam("taskDefinitionJson", taskDefinition));
        DolphinSchedulerURLBuilder dsURLBuilder = this.exportCfg.processDefinition();
        DolphinSchedulerResponse response = null;
        if (updateProcess) {
            DSWorkflowInstance wfInstance = this.loadWorkflowSPI(true);
            response = dsURLBuilder.appendSubPath(wfInstance.getSPIWorkflowId()).applyPut(params, Optional.empty());
        } else {
            response = dsURLBuilder.applyPost(params, Optional.empty());
        }

        if (!response.isSuccess()) {
            throw new IllegalStateException("processDefinition faild:" + response.errorDescribe());
        }

        logger.info("success apply url:{}", response.getApplyURL());
        final Long dsWorkflowId = response.getData().getLong(KEY_TASK_DEFINITION_CODE);

        this.setSPIWorkflowId(dsWorkflowId
                , new ExecutePhaseRange(FullbuildPhase.FullDump, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump));
    }

    private void setDisableGrpcRemoteServerConnect(JSONObject initNode) {
        initNode.put(ICoordinator.KEY_DISABLE_GRPC_REMOTE_SERVER_CONNECT, !this.exportCfg.createHistory);
    }


//    /**
//     * @param selectedTabTriggers
//     * @param unEffectiveOpt
//     * @see
//     */
//    private void innerSaveJob(
//            Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers //
//            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {
//
//        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> topologNode = selectedTabTriggers.orElseGet(() -> createWfNodes());
//        Map<ISelectedTab, SelectedTabTriggers> createWfNodesResult = topologNode.getKey();
//        WorkflowUnEffectiveJudge unEffectiveJudge = unEffectiveOpt.orElseGet(() -> new WorkflowUnEffectiveJudge());
//
//        JSONObject mrParams = null;
//
//        SelectedTabTriggers tabTriggers = null;
//        ISelectedTab selectedTab = null;
//
//        boolean containPostTrigger = false;
//
//        // Long jobId = null;
//        List<SaveWorkflowNodeRequest> wfNodes = Lists.newArrayList();
//        Optional<IWorkflowNode> changedWfNode = null;
//        for (Map.Entry<ISelectedTab, SelectedTabTriggers> entry : createWfNodesResult.entrySet()) {
//            tabTriggers = entry.getValue();
//            selectedTab = entry.getKey();
//            mrParams = tabTriggers.createMRParams();
//
//            if (tabTriggers.getPostTrigger() != null) {
//                containPostTrigger = true;
//            }
//
//            SaveJobInfoRequest jobRequest = jobTpl.createSynJobRequest();
//            changedWfNode = unEffectiveJudge.getExistWfNode(selectedTab.getName());
//
//            SaveWorkflowNodeRequest wfNode = createWorkflowNode(this.getTISPowerJob(), dataxProcessor.identityValue() + "_" + selectedTab.getName()
//                    , JsonUtil.toString(mrParams), changedWfNode, jobRequest, jobTpl);
//            wfNodes.add(wfNode);
//            jobIdMaintainer.addJob(selectedTab, wfNode.getJobId());
//        }
//
//
//        //===============================================================
//        // process startNode
//        final String startNodeName = dataxProcessor.identityValue() + KEY_START_INITIALIZE_SUFFIX;
//
//        final SaveJobInfoRequest initJobRequest = jobTpl.createInitializeJobRequest();
//        unEffectiveJudge.getStartInitNode().ifPresent((existStarNode) -> {
//            initJobRequest.setId(existStarNode.getJobId());
//        });
//        initJobRequest.setJobName(startNodeName);
//
//        JSONObject initNode = createInitNodeJson();
//
//        initJobRequest.setJobParams(JsonUtil.toString(initNode));
//
//        SaveWorkflowNodeRequest startWfNode = jobTpl.createWorkflowNode();
//        startWfNode.setJobId(result(powerJobClient.saveJob(initJobRequest)));
//        startWfNode.setNodeName(startNodeName);
//        startWfNode.setNodeParams(initJobRequest.getJobParams());
//
//        wfNodes.add(startWfNode);
//        //===============================================================
//        wfNodes.addAll(jobIdMaintainer.beforeCreateWorkflowDAG(jobTpl));
//        List<WorkflowNodeInfoDTO> savedWfNodes
//                = result(powerJobClient.saveWorkflowNode(wfNodes));
//
//
//        for (IWorkflowNode deleteNode : unEffectiveJudge.getDeletedWfNodes()) {
//            result(powerJobClient.deleteJob(deleteNode.getJobId()));
//        }
//
//        jobIdMaintainer.setStartInitJob(savedWfNodes.stream().filter((n) -> startWfNode.getJobId() == (long) n.getJobId()).findFirst());
//        jobIdMaintainer.addWorkflow(savedWfNodes);
//
//        PowerWorkflowPayload.PowerJobWorkflow powerWf = this.getPowerJobWorkflowId(false);
//
//        SaveWorkflowRequest req = jobTpl.createWorkflowRequest(dataxProcessor);
//
//        req.setId(powerWf != null ? powerWf.getPowerjobWorkflowId() : null);
//
//        PEWorkflowDAG peWorkflowDAG = jobIdMaintainer.createWorkflowDAG();
//        req.setDag(peWorkflowDAG);
//        Long saveWorkflowId = result(powerJobClient.saveWorkflow(req));
//
//        if (powerWf == null) {
//            this.setPowerJobWorkflowId(saveWorkflowId
//                    , new ExecutePhaseRange(FullbuildPhase.FullDump, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump));
//        }
//
//    }


    /**
     * <pre>
     * {
     * 	"prop": "test",
     *  "direct": "IN",
     * 	"type": "VARCHAR",
     * 	"value": "task_id"
     * }
     * </pre>
     *
     * @return
     */

    private enum ParamType {
        INTEGER, LONG, VARCHAR, BOOLEAN
    }

    private static JSONObject createParam(String key, ParamType type) {
        JSONObject inParam = new JSONObject();
        inParam.put("prop", key);
        inParam.put("direct", "OUT");
        inParam.put("type", type.name());
        return inParam;
    }

    private JSONObject createTaskDefinitionJSON(TaskGroup taskGroup, long taskCode, String name, String description) {
        JSONObject taskDefinitionJson = JsonUtil.loadJSON(DSWorkflowPayload.class, "tpl/task-definition-sync-tpl.json");
        taskDefinitionJson.put(KEY_TASK_DEFINITION_CODE, taskCode);
        taskDefinitionJson.put(KEY_TASK_DEFINITION_NAME, name);
        taskDefinitionJson.put(KEY_TASK_DESCRIPTION, description);

        taskDefinitionJson.put(KEY_TASK_GROUP_ID, taskGroup.getTaskGroupId());
        taskDefinitionJson.put(KEY_TASK_GROUP_PRIORITY, 0);
        return taskDefinitionJson;
    }


    private static JSONObject createLocation(long taskCode, int x, int y) {
        JSONObject taskLocationJson = new JSONObject();
        taskLocationJson.put(KEY_LOCATION_TASK_CODE, taskCode);
        taskLocationJson.put(KEY_LOCATION_X, x);
        taskLocationJson.put(KEY_LOCATION_Y, y);
        return taskLocationJson;
    }

    private static JSONObject createTaskRelationJSON(long preTaskId, long taskCode) {
        JSONObject taskRelationJson = JsonUtil.loadJSON(DSWorkflowPayload.class, "tpl/task-relation-tpl.json");
        if (preTaskId > 0) {
            taskRelationJson.put(KEY_RELATION_PRETASK_CODE, preTaskId);
            taskRelationJson.put(KEY_RELATION_PRETASK_VERSION, 1);
        }
        taskRelationJson.put(KEY_RELATION_POSTTASK_CODE, taskCode);
        return taskRelationJson;
    }


}
