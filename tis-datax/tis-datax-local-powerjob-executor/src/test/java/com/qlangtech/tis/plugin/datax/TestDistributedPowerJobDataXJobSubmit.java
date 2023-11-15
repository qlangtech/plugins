package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.test.TISEasyMock;
import junit.framework.TestCase;
import org.easymock.EasyMock;

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

        Application app = new Application();
        app.setAppId(999);
        app.setProjectName(testDataXName);
        EasyMock.expect(applicationDAO.selectByName(testDataXName)).andReturn(app);
        EasyMock.expect(applicationDAO.updateByExampleSelective(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(1);

        EasyMock.expect(module.getApplicationDAO()).andReturn(applicationDAO);
        Context context = mock("context", Context.class);
        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, testDataXName);

        replay();
        powerJobDataXJobSubmit.createJob(module, context, dataxProcessor);
        verifyAll();
    }

    public void testTriggerJob() {
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);
        PowerJobExecContext module = mock("moudle", PowerJobExecContext.class);
        EasyMock.expect(module.getApplicationDAO()).andReturn(applicationDAO);
        Context context = mock("context", Context.class);
        DistributedPowerJobDataXJobSubmit powerJobDataXJobSubmit = new DistributedPowerJobDataXJobSubmit();
        powerJobDataXJobSubmit.triggerJob(module, context, testDataXName);
    }


    public static interface PowerJobExecContext extends ICommonDAOContext, IControlMsgHandler {

    }


}
