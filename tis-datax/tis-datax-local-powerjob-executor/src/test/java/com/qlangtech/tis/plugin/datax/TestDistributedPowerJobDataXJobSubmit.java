package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowDAO;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.qlangtech.tis.workflow.pojo.WorkFlow;
import org.mockito.Mockito;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.Test;
import org.junit.Before;
/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/10
 */
public class TestDistributedPowerJobDataXJobSubmit implements TISEasyMock {

    static final String testDataXName = "mysql_hive3";

    @Before public void setUp() throws Exception {
        this.clearMocks();
        CenterResource.setNotFetchFromCenterRepository();
    }

    private String topplogName = "asdf";
    private Integer workflowId = 1;

    @Test
    public void testSaveJob() {
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        PowerJobExecContext module = Mockito.mock(PowerJobExecContext.class, Mockito.withSettings().name("moudle"));
        // IWorkFlowDAO workflowDAO = Mockito.mock(IWorkFlowDAO.class, Mockito.withSettings().name("workflowDAO"));
        IApplicationDAO applicationDAO = Mockito.mock(IApplicationDAO.class, Mockito.withSettings().name("applicationDAO"));
        expectApplicationSelect(applicationDAO);
        // ICommonDAOContext m = mock("tisModule", ICommonDAOContext.class);
//
//        expectApplicationSelect(applicationDAO);
        // expectWorkflowSelect(workflowDAO);

        Mockito.when(module.getApplicationDAO()).thenReturn(applicationDAO);
        Context context = Mockito.mock(Context.class, Mockito.withSettings().name("context"));
        DataxProcessor dataxProcessor =
                Mockito.mock(DataxProcessor.class, Mockito.withSettings().name("dataXProcessor"));

        DataxProcessor.processorGetter = (dataXName) -> dataxProcessor;

        Mockito.when(dataxProcessor.identityValue()).thenReturn(testDataXName);

        powerJobDataXJobSubmit.saveJob(module, context, dataxProcessor);
    }


    @Test
    public void testCreateWorkflowJob() {
        //  String topplogName = "asdf";
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        PowerJobExecContext module = Mockito.mock(PowerJobExecContext.class, Mockito.withSettings().name("moudle"));
        IWorkFlowDAO workflowDAO = Mockito.mock(IWorkFlowDAO.class, Mockito.withSettings().name("workflowDAO"));

//
//        expectApplicationSelect(applicationDAO);
        expectWorkflowSelect(workflowDAO);

        Mockito.when(module.getWorkFlowDAO()).thenReturn(workflowDAO);
        Context context = Mockito.mock(Context.class, Mockito.withSettings().name("context"));
        DataFlowDataXProcessor dataxProcessor =
                (DataFlowDataXProcessor) DataxProcessor.load(null, StoreResourceType.DataFlow, topplogName);
        IDataFlowTopology topology = dataxProcessor.getTopology();


        powerJobDataXJobSubmit.createWorkflowJob(module, context, topology);
    }

    @Test
    public void testTriggerWorkflowJob() {
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();
        PowerJobExecContext module = Mockito.mock(PowerJobExecContext.class, Mockito.withSettings().name("moudle"));
        IWorkFlowDAO workflowDAO = Mockito.mock(IWorkFlowDAO.class, Mockito.withSettings().name("workflowDAO"));
        Context context = Mockito.mock(Context.class, Mockito.withSettings().name("context"));
        IWorkflow workflow = Mockito.mock(IWorkflow.class, Mockito.withSettings().name("workflow"));
        expectWorkflowSelect(workflowDAO);
        Mockito.when(module.getWorkFlowDAO()).thenReturn(workflowDAO);

        Mockito.when(workflow.getName()).thenReturn(topplogName);

        IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = Mockito.mock(IWorkFlowBuildHistoryDAO.class, Mockito.withSettings().name("workFlowBuildHistoryDAO"));

        Mockito.when(workFlowBuildHistoryDAO.updateByExampleSelective(
                Mockito.any(), Mockito.any())).thenReturn(1);

        Mockito.when(module.getTaskBuildHistoryDAO()).thenReturn(workFlowBuildHistoryDAO);


//        DataFlowDataXProcessor dataxProcessor =
//                (DataFlowDataXProcessor) DataxProcessor.load(null, StoreResourceType.DataFlow, topplogName);
        // IDataFlowTopology topology = dataxProcessor.getTopology();

        Optional<Long> powerJobWorkflowInstanceIdOpt = Optional.empty();
//        IControlMsgHandler module, Context context,
//                IWorkflow workflow, Boolean dryRun, Optional<Long> powerJobWorkflowInstanceIdOpt

        powerJobDataXJobSubmit.triggerWorkflowJob(
                module, context, workflow, false //, powerJobWorkflowInstanceIdOpt
                , Optional.empty());
    }

    private void expectWorkflowSelect(IWorkFlowDAO workflowDAO) {
        WorkFlow wf = new WorkFlow();
        wf.setId(workflowId);
        wf.setName(topplogName);
        wf.setOpTime(new Date());
        wf.setCreateTime(new Date());
        Long powerjobWorkflowId = 41l;

        ExecutePhaseRange executePhaseRange = new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN);

        wf.setGitPath(JsonUtil.toString(
                BasicWorkflowPayload.createPayload(powerjobWorkflowId, executePhaseRange, new JSONObject())));

        List<WorkFlow> workFlows = Lists.newArrayList(wf);

        Mockito.when(workflowDAO.selectByExample(Mockito.any())).thenReturn(workFlows);

        Mockito.when(workflowDAO.updateByExampleSelective(
                Mockito.any(), Mockito.any())).thenReturn(1);
    }


    @Test
    public void testCreateJob() {

        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        PowerJobExecContext module = Mockito.mock(PowerJobExecContext.class, Mockito.withSettings().name("moudle"));
        IApplicationDAO applicationDAO = Mockito.mock(IApplicationDAO.class, Mockito.withSettings().name("applicationDAO"));

        expectApplicationSelect(applicationDAO);


        Mockito.when(module.getApplicationDAO()).thenReturn(applicationDAO);
        Context context = Mockito.mock(Context.class, Mockito.withSettings().name("context"));
        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, testDataXName);

        powerJobDataXJobSubmit.createJob(module, context, dataxProcessor);
    }

    private static void expectApplicationSelect(IApplicationDAO applicationDAO) {
        Application app = new Application();
        app.setAppId(999);
        app.setProjectName(testDataXName);
        Mockito.when(applicationDAO.selectByName(testDataXName)).thenReturn(app);

        Mockito.when(applicationDAO.updateByExampleSelective(
                Mockito.any(), Mockito.any())).thenReturn(1);
    }

    @Test
    public void testTriggerJob() {
        IApplicationDAO applicationDAO = Mockito.mock(IApplicationDAO.class, Mockito.withSettings().name("applicationDAO"));

        expectApplicationSelect(applicationDAO);
        PowerJobExecContext module = Mockito.mock(PowerJobExecContext.class, Mockito.withSettings().name("moudle"));


        IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = Mockito.mock(IWorkFlowBuildHistoryDAO.class, Mockito.withSettings().name("workFlowBuildHistoryDAO"));

        Mockito.when(workFlowBuildHistoryDAO.updateByExampleSelective(
                Mockito.any(), Mockito.any())).thenReturn(1);

        Mockito.when(module.getTaskBuildHistoryDAO()).thenReturn(workFlowBuildHistoryDAO);
        Mockito.when(module.getApplicationDAO()).thenReturn(applicationDAO);
        Context context = Mockito.mock(Context.class, Mockito.withSettings().name("context"));
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        HttpUtils.addMockApply(0, "do_create_new_task"
                , "create_new_task_single_table_index_build_response.json", TestDistributedPowerJobDataXJobSubmit.class);
        DataXName dataXPipeline = DataXName.createDataXPipeline(testDataXName);
        powerJobDataXJobSubmit.triggerJob(null,
                 dataXPipeline);
    }


    public static interface PowerJobExecContext extends ICommonDAOContext, IControlMsgHandler {

    }


}
