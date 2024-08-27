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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.assemble.TriggerType;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.PowerjobTriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.phasestatus.PhaseStatusCollection;
import com.qlangtech.tis.fullbuild.phasestatus.impl.AbstractChildProcessStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.DumpPhaseStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.biz.dal.pojo.ApplicationCriteria;
import com.qlangtech.tis.manage.common.CreateNewTaskResult;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit.WorkflowNodeVisit;
import com.qlangtech.tis.plugin.datax.powerjob.SPIWrokerMemorySpec;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.trigger.JobTrigger;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.sql.parser.DAGSessionSpec;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.qlangtech.tis.fullbuild.IFullBuildContext.KEY_WORKFLOW_ID;
import static com.qlangtech.tis.plugin.datax.BasicWorkflowInstance.vistWorkflowNodes;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 14:24
 **/
public abstract class BasicWorkflowPayload<WF_INSTANCE extends BasicWorkflowInstance> {
    public static final String EXEC_RANGE = "execRange";
    private static final Logger logger = LoggerFactory.getLogger(BasicWorkflowPayload.class);
    protected final IDataxProcessor dataxProcessor;
    protected final BasicDistributedSPIDataXJobSubmit submit;
    protected final ICommonDAOContext commonDAOContext;

    public BasicWorkflowPayload(IDataxProcessor dataxProcessor, ICommonDAOContext commonDAOContext, BasicDistributedSPIDataXJobSubmit submit) {
        this.dataxProcessor = dataxProcessor;
        this.submit = submit;
        this.commonDAOContext = commonDAOContext;
    }

    public static JSONObject createPayload( //
                                            Long powerjobWorkflowId, ExecutePhaseRange executePhaseRange, JSONObject appPayload) {
        appPayload.put(KEY_WORKFLOW_ID, Objects.requireNonNull(powerjobWorkflowId, "param powerjobWorkflowId can not be null"));
        appPayload.put(EXEC_RANGE, new String[]{Objects.requireNonNull(
                executePhaseRange, "param executePhaseRange can not be null").getStart().getName()
                , executePhaseRange.getEnd().getName()});
        return appPayload;
    }

    protected final WF_INSTANCE loadWorkflowSPI() {
        return this.loadWorkflowSPI(false);
    }


    /**
     * @param validateWorkflowId 是否校验证
     * @return
     */
    public final WF_INSTANCE loadWorkflowSPI(boolean validateWorkflowId) {
        JSONObject payload = getAppPayload();
        Long spiWorkflowId = payload.getLong(KEY_WORKFLOW_ID);
        if (validateWorkflowId) {
            Objects.requireNonNull(spiWorkflowId
                    , "param " + KEY_WORKFLOW_ID + " can not be null");
        }

        if (spiWorkflowId == null) {
            return null;
        }

        JSONArray execRange = Objects.requireNonNull(payload.getJSONArray(EXEC_RANGE)
                , "key:" + EXEC_RANGE + " relevant props can not be null");

        if (execRange.size() != 2) {
            throw new IllegalStateException("execRange.size() must be 2 ,but now is " + JsonUtil.toString(execRange));
        }

        return createWorkflowInstance(spiWorkflowId, new ExecutePhaseRange( //
                FullbuildPhase.parse(execRange.getString(0)) //
                , FullbuildPhase.parse(execRange.getString(1))));
    }

    protected abstract WF_INSTANCE createWorkflowInstance(Long spiWorkflowId, ExecutePhaseRange execRange);


    protected abstract String getPayloadContent();

    protected final JSONObject getAppPayload() {
        JSONObject payload = null;
        String payloadContent = null;
        try {
            payloadContent = getPayloadContent();
            payload = JSONObject.parseObject(payloadContent);
        } catch (Throwable e) {
            logger.warn("payloadContent:" + payloadContent);
        }
        return payload == null ? new JSONObject() : payload;
    }

//    /**
//     * @param daoContext
//     * @param workflowInstanceIdOpt TIS 中触发历史taskId
//     * @param feedback
//     * @return
//     */
//    public abstract TriggerBuildResult triggerWorkflow(ICommonDAOContext daoContext, Optional<Long> workflowInstanceIdOpt
//            , StatusRpcClientFactory.AssembleSvcCompsite feedback);

    public abstract void innerCreatePowerjobWorkflow(
            Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers //
            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt);


    //  protected abstract List<IWorkflowNode> getDAGNodes(BasicWorkflowInstance powerJobWorkflowId);

    protected abstract SPIExecContext createSPIExecContext();

    /**
     * @param workflowInstanceIdOpt TIS 中触发历史taskId
     * @param feedback
     * @return
     */
    public PowerjobTriggerBuildResult triggerWorkflow(Optional<Long> workflowInstanceIdOpt
            , StatusRpcClientFactory.AssembleSvcCompsite feedback) {
        //  Objects.requireNonNull(statusRpc, "statusRpc can not be null");

        WorkflowSPIInitializer<BasicWorkflowInstance> workflowInitializer = new WorkflowSPIInitializer(this);

        BasicWorkflowInstance spiWorkflowId = workflowInitializer.initialize();

        List<IWorkflowNode> wfNodes = Objects.requireNonNull(spiWorkflowId, "spiWorkflowId can not be null").getWorkflowNodes();// getDAGNodes(spiWorkflowId);
        final List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs = Lists.newArrayList();
        final List<ISqlTask.SqlTaskCfg> joinNodeCfgs = Lists.newArrayList();
        vistWorkflowNodes(this.dataxProcessor.identityValue(), wfNodes, new WorkflowNodeVisit() {
            @Override
            public void vistStartInitNode(IWorkflowNode node) {
                return;
            }

            @Override
            public void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, IWorkflowNode node) {
                joinNodeCfgs.add(cfg);
            }

            @Override
            public void vistDumpWorkerNode(IWorkflowNode node) {
                triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
            }
        });

        SPIExecContext chainContext = this.createSPIExecContext();
        chainContext.setExecutePhaseRange(spiWorkflowId.getExecutePhaseRange());
        //
        /**===================================================================
         * 创建 TIS的taskId
         ===================================================================*/
        CreateNewTaskResult newTaskResult
                = this.commonDAOContext.createNewDataXTask(chainContext, workflowInstanceIdOpt.isPresent() ? TriggerType.CRONTAB : TriggerType.MANUAL);

        final Integer tisTaskId = newTaskResult.getTaskid();

        if (CollectionUtils.isEmpty(triggerCfgs)) {
            throw new IllegalStateException("powerjob workflowId:" + spiWorkflowId.getSPIWorkflowId()
                    + " relevant nodes triggerCfgs can not be null empty");
        }

        PhaseStatusCollection statusCollection = createPhaseStatus(spiWorkflowId, triggerCfgs, joinNodeCfgs, tisTaskId);
        feedback.initSynJob(statusCollection);

        JSONObject instanceParams = createInstanceParams(tisTaskId);
        // 取得powerjob instanceId
        Long workflowInstanceIdOfSPI = workflowInstanceIdOpt.orElseGet(() -> {
            /****************************************
             * 手动触发的情况
             ****************************************/
            Long createWorkflowInstanceId = runSPIWorkflow(spiWorkflowId, instanceParams);
            logger.info("create workflow instanceId:{}", createWorkflowInstanceId);
            return createWorkflowInstanceId;
        });


        WorkFlowBuildHistoryPayload buildHistoryPayload = createBuildHistoryPayload(tisTaskId);

        buildHistoryPayload.setSPIWorkflowInstanceId(workflowInstanceIdOfSPI);

        PowerjobTriggerBuildResult buildResult = new PowerjobTriggerBuildResult(true, instanceParams);
        buildResult.taskid = tisTaskId;

        initializeService(commonDAOContext);
        triggrWorkflowJobs.offer(buildHistoryPayload);
        if (checkWorkflowJobsLock.tryLock()) {

            scheduledExecutorService.schedule(() -> {
                checkWorkflowJobsLock.lock();
                try {
                    int count = 0;
                    WorkFlowBuildHistoryPayload pl = null;
                    List<WorkFlowBuildHistoryPayload> checkWf = Lists.newArrayList();
                    ExecResult execResult = null;
                    // TISPowerJobClient powerJobClient = this.getTISPowerJob();
                    while (true) {
                        while ((pl = triggrWorkflowJobs.poll()) != null) {
                            checkWf.add(pl);
                            count++;
                        }

                        if (CollectionUtils.isEmpty(checkWf)) {
                            logger.info("the turn all of the powerjob workflow job has been terminated,jobs count:{}", count);
                            return;
                        }

                        // WorkflowInstanceInfoDTO wfStatus = null;
                        Iterator<WorkFlowBuildHistoryPayload> it = checkWf.iterator();
                        WorkFlowBuildHistoryPayload p;
                        int allWfJobsCount = checkWf.size();
                        int removed = 0;
                        while (it.hasNext()) {
                            p = it.next();
                            if ((execResult = p.processExecHistoryRecord()) != null) {
                                // 说明结束了
                                it.remove();
                                removed++;
                                // 正常结束？ 还是失败导致？
                                if (execResult != ExecResult.SUCCESS) {

                                }
                                triggrWorkflowJobs.taskFinal(p, execResult);
                            }
                        }
                        logger.info("start to wait next time to check job status,to terminate status count:{},allWfJobsCount:{}", removed, allWfJobsCount);
                        try {
                            Thread.sleep(4000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    checkWorkflowJobsLock.unlock();
                }
            }, 5, TimeUnit.SECONDS);
            checkWorkflowJobsLock.unlock();
        }


        return buildResult;
    }

    protected abstract WorkFlowBuildHistoryPayload createBuildHistoryPayload(Integer tisTaskId);


    private static void initializeService(ICommonDAOContext daoContext) {
        try {
            if (scheduledExecutorService == null || triggrWorkflowJobs == null) {
                synchronized (BasicWorkflowPayload.class) {
                    if (scheduledExecutorService == null) {
                        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                    }

                    if (triggrWorkflowJobs == null) {
                        triggrWorkflowJobs = TriggrWorkflowJobs.create(daoContext);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private transient static ScheduledExecutorService scheduledExecutorService;
    private transient static final ReentrantLock checkWorkflowJobsLock = new ReentrantLock();
    private transient static TriggrWorkflowJobs triggrWorkflowJobs;


    protected abstract Long runSPIWorkflow(BasicWorkflowInstance spiWorkflowId, JSONObject instanceParams);


    public Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>
    createWfNodes() {
        RpcServiceReference rpcStub = StatusRpcClientFactory.getMockStub();
        Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers = Maps.newHashMap();
        SPIExecContext execChainContext = null;

        SelectedTabTriggers tabTriggers = null;
        PowerJobTskTriggers tskTriggers = null;

        Optional<JobTrigger> partialTrigger = Optional.empty();
        /**
         * FIXME 先去掉以powerjob执行方式部份表的需求
         * partialTrigger =  JobTrigger.getTriggerFromHttpParam(execChainContext)
         */
        DataXCfgGenerator.GenerateCfgs cfgFileNames
                = dataxProcessor.getDataxCfgFileNames(null, partialTrigger);
        for (IDataxReader reader : dataxProcessor.getReaders(null)) {
            for (ISelectedTab selectedTab : reader.getSelectedTabs()) {

                /**
                 * FIXME 先去掉，原因如上
                 if (!cfgFileNames.getTargetTabs().contains(selectedTab.getName())) {
                 continue;
                 }
                 */
                execChainContext = new SPIExecContext();
                tskTriggers = new PowerJobTskTriggers();
                execChainContext.setAppname(dataxProcessor.identityValue());
                execChainContext.setTskTriggers(tskTriggers);
                execChainContext.setAttribute(JobCommon.KEY_TASK_ID, -1);


                tskTriggers = new PowerJobTskTriggers();
                execChainContext.setTskTriggers(tskTriggers);


                DAGSessionSpec sessionSpec = new DAGSessionSpec();

                tabTriggers = DAGSessionSpec.buildTaskTriggers(
                        execChainContext, dataxProcessor, submit, rpcStub, selectedTab, selectedTab.getName(),
                        sessionSpec, cfgFileNames);

                selectedTabTriggers.put(selectedTab, tabTriggers);
            }
        }

        if (dataxProcessor.getResType() == StoreResourceType.DataFlow) {
            dataxProcessor.identityValue();
        }

        return Pair.of(selectedTabTriggers, Collections.emptyMap());
    }

    protected final JSONObject createInstanceParams(Integer tisTaskId) {
        try {
            JSONObject instanceParams = IExecChainContext.createInstanceParams(tisTaskId, dataxProcessor, false, Optional.empty());

            DataXJobWorker worker = getSPIJobWorker();
            if (worker != null) {
                ReplicasSpec replicasSpec = worker.getReplicasSpec();
                instanceParams.put(JobParams.KEY_JAVA_MEMORY_SPEC
                        , replicasSpec.toJavaMemorySpec(Optional.of(SPIWrokerMemorySpec.dataXExecutorMemoryProportion())));
            }

            WorkFlowBuildHistory latestSuccessWorkflowHistory = this.commonDAOContext.getLatestSuccessWorkflowHistory(dataxProcessor.getResTarget());

            if (latestSuccessWorkflowHistory != null) {
                instanceParams.put(JobParams.KEY_PREVIOUS_TASK_ID, latestSuccessWorkflowHistory.getId());
            }

            return instanceParams;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected DataXJobWorker getSPIJobWorker() {
        return DataXJobWorker.getK8SDataXPowerJobWorker();
    }

    private PhaseStatusCollection createPhaseStatus(BasicWorkflowInstance spiWorkflowId
            , List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs //
            , List<ISqlTask.SqlTaskCfg> joinNodeCfgs //
            , Integer tisTaskId) {
        PhaseStatusCollection statusCollection = new PhaseStatusCollection(tisTaskId, spiWorkflowId.getExecutePhaseRange());
        DumpPhaseStatus dumpPhase = new DumpPhaseStatus(tisTaskId);
        JoinPhaseStatus joinPhase = new JoinPhaseStatus(tisTaskId);
        statusCollection.setDumpPhase(dumpPhase);
        statusCollection.setJoinPhase(joinPhase);

        for (SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg : triggerCfgs) {

            if (StringUtils.isNotEmpty(triggerCfg.getPreTrigger())) {
                setInitStatus(dumpPhase.getTable(triggerCfg.getPreTrigger()));
            }

            if (StringUtils.isNotEmpty(triggerCfg.getPostTrigger())) {
                setInitStatus(joinPhase.getTaskStatus(triggerCfg.getPostTrigger()));
            }

            for (CuratorDataXTaskMessage taskMsg : triggerCfg.getSplitTabsCfg()) {
                setInitStatus(dumpPhase.getTable(DataXJobInfo.parse(taskMsg.getJobName()).jobFileName));
            }
        }

        for (ISqlTask.SqlTaskCfg joinTskCfg //
                : Objects.requireNonNull(joinNodeCfgs, "joinNodeCfgs can not be null")) {
            setInitStatus(joinPhase.getTaskStatus(joinTskCfg.getExportName()));
        }

        return statusCollection;
    }

    private void setInitStatus(AbstractChildProcessStatus status) {
        status.setFaild(false);
        status.setWaiting(true);
        status.setComplete(false);
    }

    protected JSONObject createInitNodeJson() {
        JSONObject initNode = new JSONObject();
        initNode.put(DataxUtils.DATAX_NAME, dataxProcessor.identityValue());
        // 是否是dataflow的处理类型
        initNode.put(DataxUtils.TIS_WORK_FLOW_CHANNEL, dataxProcessor.getResType() == StoreResourceType.DataFlow);
        return initNode;
    }

    public void setSPIWorkflowId(Long workflowId, ExecutePhaseRange executePhaseRange) {
        Objects.requireNonNull(workflowId, "param workflowId can not be null");
        JSONObject appPayload = getAppPayload();
        createPayload(workflowId, executePhaseRange, appPayload);
        setSPIWorkflowPayload(appPayload);

    }

    protected abstract void setSPIWorkflowPayload(JSONObject appPayload);

    public static class SPIWorkflowPayloadApplicationStore {
        private final IApplicationDAO applicationDAO;
        private final String appName;
        private Application app;

        public SPIWorkflowPayloadApplicationStore(String appName, ICommonDAOContext commonDAOContext) {
            Objects.requireNonNull(commonDAOContext, "commonDAOContext can not be null");
            this.applicationDAO = Objects.requireNonNull(commonDAOContext.getApplicationDAO(), "applicationDAO can not be null");
            this.appName = appName;
        }

        public Application load() {
            if (this.app == null) {
                this.app = Objects.requireNonNull(//
                        applicationDAO.selectByName(appName), "appName:" + appName + " relevant app can not be null");
            }
            return this.app;
        }

        public String getPayloadContent() {
            return this.load().getFullBuildCronTime();
        }

        public void setSPIWorkflowPayload(JSONObject appPayload) {
            Application app = new Application();
            app.setFullBuildCronTime(JsonUtil.toString(Objects.requireNonNull(appPayload, "appPayload can not be null")));

            Application application = this.load();
            application.setFullBuildCronTime(app.getFullBuildCronTime());
            ApplicationCriteria appCriteria = new ApplicationCriteria();
            appCriteria.createCriteria().andAppIdEqualTo(application.getAppId());
            if (applicationDAO.updateByExampleSelective(app, appCriteria) < 1) {
                throw new IllegalStateException("app:" + application.getProjectName() + " update workflowId in payload faild");
            }
            this.app = null;
        }
    }
}
