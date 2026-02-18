package com.qlangtech.tis.plugin.akka;

import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.dag.TISActorSystem;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.impl.DataXPipelineExecContext;
import com.qlangtech.tis.job.common.IPipelineExecContext;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.workflow.dao.IDAGNodeExecutionDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.DagNodeExecution;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.qlangtech.tis.workflow.pojo.WorkflowDAGFileManager;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/2/1
 * @see DistributedAKKAJobDataXJobSubmit
 */
public class DistributedAKKAJobDataXJobSubmitTest implements TISEasyMock {

    //    static final String testDataXName = "mysql_hive3";
    static final String testDataXName = "mysql_mysql";

    @Before
    public void beforeExecute() {
        ITISCoordinator.disableRemoteServer();
        CenterResource.setNotFetchFromCenterRepository();

    }

    @Test
    public void testTriggerJob() throws Exception {
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);
        IPipelineExecContext execContext = mock("pipelineExecContext", IPipelineExecContext.class);
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, testDataXName);

        int newCreateTaskId = 9999;

        EasyMock.expect(execContext.getTaskId()).andReturn(newCreateTaskId);

        IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO = mock("workflowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);
        // EasyMock.expect(workflowBuildHistoryDAO.insertSelective(EasyMock.anyObject(WorkFlowBuildHistory.class))).andReturn(newCreateTaskId).anyTimes();
        EasyMock.expect(workflowBuildHistoryDAO.updateByPrimaryKeySelective(EasyMock.anyObject(WorkFlowBuildHistory.class))).andReturn(1).anyTimes();
        WorkFlowBuildHistory buildHistory = new WorkFlowBuildHistory();
        File dagRuntime = new File(dataxProcessor.getDataXWorkDir(null), WorkflowDAGFileManager.DAG_SPEC_FILENAME);// "/opt/data/tis/cfg_repo/tis_plugin_config/ap/mysql_mysql/dag-spec.json";
        buildHistory.setDagRuntime(dagRuntime.getAbsolutePath());
        EasyMock.expect(workflowBuildHistoryDAO.loadFromWriteDB(newCreateTaskId)).andReturn(buildHistory);

//        EasyMock.expect(workflowBuildHistoryDAO.updateByExampleSelective(//
//                EasyMock.anyObject(WorkFlowBuildHistory.class), EasyMock.anyObject(WorkFlowBuildHistoryCriteria.class))).andReturn(1);

        IDAGNodeExecutionDAO dagNodeExecutionDAO = mock("dagNodeExecutionDAO", IDAGNodeExecutionDAO.class);
        EasyMock.expect(dagNodeExecutionDAO.insertSelective(EasyMock.anyObject(DagNodeExecution.class))) //
                .andReturn(1).anyTimes();
        //
        //  TISActorSystem.createAndInit(DAOFacade.createAKKAClusterDependenceDao());
        TISActorSystem.createAndInit(new DAORestDelegateFacade(workflowBuildHistoryDAO, dagNodeExecutionDAO));
        expectApplicationSelect(applicationDAO);
        MockExecContext module = mock("moudle", MockExecContext.class);
        Long triggerTimestamp = System.currentTimeMillis();

        DataXPipelineExecContext pipelineExecContext = new DataXPipelineExecContext(testDataXName, triggerTimestamp);
        // IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = mock("workFlowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);

//        EasyMock.expect(workflowBuildHistoryDAO.updateByExampleSelective(
//                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(2);

        EasyMock.expect(module.getTaskBuildHistoryDAO()).andReturn(workflowBuildHistoryDAO);
        EasyMock.expect(module.getApplicationDAO()).andReturn(applicationDAO).anyTimes();
        //Context context = mock("context", Context.class);
        DistributedAKKAJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedAKKAJobDataXJobSubmit();


        HttpUtils.addMockApply(0, "do_create_new_task"
                , "create_new_task_single_table_index_build_response.json", DistributedAKKAJobDataXJobSubmitTest.class);
        DataXName dataXPipeline = DataXName.createDataXPipeline(testDataXName);

        this.replay();

        powerJobDataXJobSubmit.triggerJob(pipelineExecContext,
                dataXPipeline, Optional.empty());


        Thread.sleep(999000);
        this.verifyAll();
    }

    private static void expectApplicationSelect(IApplicationDAO applicationDAO) {
        Application app = new Application();
        app.setAppId(999);
        app.setProjectName(testDataXName);
        EasyMock.expect(applicationDAO.selectByName(testDataXName)).andReturn(app).anyTimes();

        EasyMock.expect(applicationDAO.updateByExampleSelective(
                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(1).anyTimes();
    }

    public static interface MockExecContext extends ICommonDAOContext, IControlMsgHandler {

    }
}