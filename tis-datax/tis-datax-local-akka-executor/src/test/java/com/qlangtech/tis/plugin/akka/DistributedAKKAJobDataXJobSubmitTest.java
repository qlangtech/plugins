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
import org.mockito.Mockito;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

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
        IApplicationDAO applicationDAO = Mockito.mock(IApplicationDAO.class, Mockito.withSettings().name("applicationDAO"));
        IPipelineExecContext execContext = Mockito.mock(IPipelineExecContext.class, Mockito.withSettings().name("pipelineExecContext"));
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, testDataXName);

        int newCreateTaskId = 9999;

        Mockito.when(execContext.getTaskId()).thenReturn(newCreateTaskId);

        IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO = Mockito.mock(IWorkFlowBuildHistoryDAO.class, Mockito.withSettings().name("workflowBuildHistoryDAO"));
        // EasyMock.expect(workflowBuildHistoryDAO.insertSelective(EasyMock.anyObject(WorkFlowBuildHistory.class))).andReturn(newCreateTaskId).anyTimes();
        Mockito.when(workflowBuildHistoryDAO.updateByPrimaryKeySelective(Mockito.any(WorkFlowBuildHistory.class))).thenReturn(1);
        WorkFlowBuildHistory buildHistory = new WorkFlowBuildHistory();
        File dagRuntime = new File(dataxProcessor.getDataXWorkDir(null), WorkflowDAGFileManager.DAG_SPEC_FILENAME);// "/opt/data/tis/cfg_repo/tis_plugin_config/ap/mysql_mysql/dag-spec.json";
        buildHistory.setDagRuntime(dagRuntime.getAbsolutePath());
        Mockito.when(workflowBuildHistoryDAO.loadFromWriteDB(newCreateTaskId)).thenReturn(buildHistory);

//        EasyMock.expect(workflowBuildHistoryDAO.updateByExampleSelective(//
//                EasyMock.anyObject(WorkFlowBuildHistory.class), EasyMock.anyObject(WorkFlowBuildHistoryCriteria.class))).andReturn(1);

        IDAGNodeExecutionDAO dagNodeExecutionDAO = Mockito.mock(IDAGNodeExecutionDAO.class, Mockito.withSettings().name("dagNodeExecutionDAO"));
        Mockito.when(dagNodeExecutionDAO.insertSelective(Mockito.any(DagNodeExecution.class))) //
                .thenReturn(1);
        //
        //  TISActorSystem.createAndInit(DAOFacade.createAKKAClusterDependenceDao());
        TISActorSystem.createAndInit(new DAORestDelegateFacade(workflowBuildHistoryDAO, dagNodeExecutionDAO));
        expectApplicationSelect(applicationDAO);
        MockExecContext module = Mockito.mock(MockExecContext.class, Mockito.withSettings().name("moudle"));
        Long triggerTimestamp = System.currentTimeMillis();

        DataXPipelineExecContext pipelineExecContext = new DataXPipelineExecContext(testDataXName, triggerTimestamp);
        // IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = mock("workFlowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);

//        EasyMock.expect(workflowBuildHistoryDAO.updateByExampleSelective(
//                EasyMock.anyObject(), EasyMock.anyObject())).andReturn(2);

        Mockito.when(module.getTaskBuildHistoryDAO()).thenReturn(workflowBuildHistoryDAO);
        Mockito.when(module.getApplicationDAO()).thenReturn(applicationDAO);
        //Context context = mock("context", Context.class);
        DistributedAKKAJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedAKKAJobDataXJobSubmit();


        HttpUtils.addMockApply(0, "do_create_new_task"
                , "create_new_task_single_table_index_build_response.json", DistributedAKKAJobDataXJobSubmitTest.class);
        DataXName dataXPipeline = DataXName.createDataXPipeline(testDataXName);

        powerJobDataXJobSubmit.triggerJob(pipelineExecContext,
                dataXPipeline);


        Thread.sleep(999000);
    }

    private static void expectApplicationSelect(IApplicationDAO applicationDAO) {
        Application app = new Application();
        app.setAppId(999);
        app.setProjectName(testDataXName);
        Mockito.when(applicationDAO.selectByName(testDataXName)).thenReturn(app);

        Mockito.when(applicationDAO.updateByExampleSelective(
                Mockito.any(), Mockito.any())).thenReturn(1);
    }

    public static interface MockExecContext extends ICommonDAOContext, IControlMsgHandler {

    }
}