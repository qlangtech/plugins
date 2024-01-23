package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowDAO;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.qlangtech.tis.workflow.pojo.WorkFlow;
import junit.framework.TestCase;
import org.easymock.EasyMock;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/10
 */
public class TestDistributedPowerJobDataXJobSubmit extends TestCase implements TISEasyMock {

    static final String testDataXName = "mysql_hive3";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.clearMocks();
        CenterResource.setNotFetchFromCenterRepository();
    }

    private String topplogName = "asdf";
    private Integer workflowId = 1;

    public void testSaveJob() {
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        PowerJobExecContext module = mock("moudle", PowerJobExecContext.class);
        // IWorkFlowDAO workflowDAO = mock("workflowDAO", IWorkFlowDAO.class);
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);
        expectApplicationSelect(applicationDAO);
        // ICommonDAOContext m = mock("tisModule", ICommonDAOContext.class);
//
//        expectApplicationSelect(applicationDAO);
        // expectWorkflowSelect(workflowDAO);

        EasyMock.expect(module.getApplicationDAO()).andReturn(applicationDAO);
        Context context = mock("context", Context.class);
        DataxProcessor dataxProcessor =
                mock("dataXProcessor", DataxProcessor.class);

        DataxProcessor.processorGetter = (dataXName) -> dataxProcessor;

        EasyMock.expect(dataxProcessor.identityValue()).andReturn(testDataXName).anyTimes();

        replay();
        powerJobDataXJobSubmit.saveJob(module, context, dataxProcessor);
        verifyAll();
    }


    public void testCreateWorkflowJob() {
        //  String topplogName = "asdf";
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        PowerJobExecContext module = mock("moudle", PowerJobExecContext.class);
        IWorkFlowDAO workflowDAO = mock("workflowDAO", IWorkFlowDAO.class);

//
//        expectApplicationSelect(applicationDAO);
        expectWorkflowSelect(workflowDAO);

        EasyMock.expect(module.getWorkFlowDAO()).andReturn(workflowDAO);
        Context context = mock("context", Context.class);
        DataFlowDataXProcessor dataxProcessor =
                (DataFlowDataXProcessor) DataxProcessor.load(null, StoreResourceType.DataFlow, topplogName);
        IDataFlowTopology topology = dataxProcessor.getTopology();
        replay();


        powerJobDataXJobSubmit.createWorkflowJob(module, context, topology);
        verifyAll();
    }

    public void testTriggerWorkflowJob() {
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();
        PowerJobExecContext module = mock("moudle", PowerJobExecContext.class);
        IWorkFlowDAO workflowDAO = mock("workflowDAO", IWorkFlowDAO.class);
        Context context = mock("context", Context.class);
        IWorkflow workflow = mock("workflow", IWorkflow.class);
        expectWorkflowSelect(workflowDAO);
        EasyMock.expect(module.getWorkFlowDAO()).andReturn(workflowDAO);

        EasyMock.expect(workflow.getName()).andReturn(topplogName);

        IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = mock("workFlowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);

        EasyMock.expect(workFlowBuildHistoryDAO.updateByExampleSelective(
                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(1);

        EasyMock.expect(module.getTaskBuildHistoryDAO()).andReturn(workFlowBuildHistoryDAO);


//        DataFlowDataXProcessor dataxProcessor =
//                (DataFlowDataXProcessor) DataxProcessor.load(null, StoreResourceType.DataFlow, topplogName);
        // IDataFlowTopology topology = dataxProcessor.getTopology();
        replay();

        Optional<Long> powerJobWorkflowInstanceIdOpt = Optional.empty();
//        IControlMsgHandler module, Context context,
//                IWorkflow workflow, Boolean dryRun, Optional<Long> powerJobWorkflowInstanceIdOpt

        powerJobDataXJobSubmit.triggerWorkflowJob(module, context, workflow, false, powerJobWorkflowInstanceIdOpt);
        verifyAll();
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
                PowerWorkflowPayload.createPayload(powerjobWorkflowId, executePhaseRange, new JSONObject())));

        List<WorkFlow> workFlows = Lists.newArrayList(wf);

        EasyMock.expect(workflowDAO.selectByExample(EasyMock.anyObject())).andReturn(workFlows);

        EasyMock.expect(workflowDAO.updateByExampleSelective(
                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(1).anyTimes();
    }


    public void testCreateJob() {

        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        PowerJobExecContext module = mock("moudle", PowerJobExecContext.class);
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);

        expectApplicationSelect(applicationDAO);


        EasyMock.expect(module.getApplicationDAO()).andReturn(applicationDAO);
        Context context = mock("context", Context.class);
        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, testDataXName);

        replay();
        powerJobDataXJobSubmit.createJob(module, context, dataxProcessor);
        verifyAll();
    }

    private static void expectApplicationSelect(IApplicationDAO applicationDAO) {
        Application app = new Application();
        app.setAppId(999);
        app.setProjectName(testDataXName);
        EasyMock.expect(applicationDAO.selectByName(testDataXName)).andReturn(app).anyTimes();

        EasyMock.expect(applicationDAO.updateByExampleSelective(
                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(1).anyTimes();
    }

    public void testTriggerJob() {
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);

        expectApplicationSelect(applicationDAO);
        PowerJobExecContext module = mock("moudle", PowerJobExecContext.class);


        IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = mock("workFlowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);

        EasyMock.expect(workFlowBuildHistoryDAO.updateByExampleSelective(
                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(1);

        EasyMock.expect(module.getTaskBuildHistoryDAO()).andReturn(workFlowBuildHistoryDAO);
        EasyMock.expect(module.getApplicationDAO()).andReturn(applicationDAO).anyTimes();
        Context context = mock("context", Context.class);
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();

        this.replay();

        HttpUtils.addMockApply(0, "do_create_new_task"
                , "create_new_task_single_table_index_build_response.json", TestDistributedPowerJobDataXJobSubmit.class);

        powerJobDataXJobSubmit.triggerJob(module, context, testDataXName, Optional.empty());

        this.verifyAll();
    }


    public static interface PowerJobExecContext extends ICommonDAOContext, IControlMsgHandler {

    }


}
