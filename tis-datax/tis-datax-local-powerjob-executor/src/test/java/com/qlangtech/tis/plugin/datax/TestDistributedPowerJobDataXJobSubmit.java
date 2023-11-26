package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import junit.framework.TestCase;
import org.easymock.EasyMock;

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
