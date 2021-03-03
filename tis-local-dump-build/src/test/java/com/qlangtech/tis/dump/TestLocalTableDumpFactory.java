package com.qlangtech.tis.dump;

import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.cloud.MockZKUtils;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.MockTaskContextUtils;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.fullbuild.indexbuild.TaskContext;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.order.dump.task.ITestDumpCommon;
import com.qlangtech.tis.order.dump.task.MockDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;

import java.io.File;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author: baisui 百岁
 * @create: 2021-03-03 13:52
 **/
public class TestLocalTableDumpFactory extends TestCase implements ITestDumpCommon {

    public void testSingleTableDump() throws Exception {

        LocalTableDumpFactory tableDumpFactory = new LocalTableDumpFactory();
        File dumpRoot = new File(Config.getDataDir(), "dumptable");
        FileUtils.forceMkdir(dumpRoot);
        tableDumpFactory.rootDir = dumpRoot.getAbsolutePath();
        tableDumpFactory.name = "test";

        DataSourceFactory mockEmployeesDataSource = MockDataSourceFactory.getMockEmployeesDataSource();

        tableDumpFactory.setDataSourceFactoryGetter((tab) -> {
            return mockEmployeesDataSource;
        });

        TaskContext taskContext = MockTaskContextUtils.create(new Date());
        ITISCoordinator zkCoordinator = MockZKUtils.createZkMock();

        taskContext.setCoordinator(zkCoordinator);
        EasyMock.replay(zkCoordinator);
        IRemoteJobTrigger singleTableDumpJob = tableDumpFactory.createSingleTableDumpJob(this.getEmployeeTab(), taskContext);

        /** -----------------------------------------------------------
         * 开始执行数据导入流程
         -----------------------------------------------------------*/
        singleTableDumpJob.submitJob();

        final RunningStatus runningStatus = singleTableDumpJob.getRunningStatus();
        assertNotNull(runningStatus);
        CountDownLatch countDown = new CountDownLatch(1);
        Runnable waitThread = () -> {
            try {
                RunningStatus s = runningStatus;
                while (!s.isComplete()) {
                    Thread.sleep(1000);
                    s = singleTableDumpJob.getRunningStatus();
                }
                assertTrue("s.isComplete()", s.isComplete());
                assertTrue("s.isSuccess()", s.isSuccess());

                countDown.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {

            }
        };

        Thread t = new Thread(waitThread);
        Object[] errs = new Object[1];
        t.setUncaughtExceptionHandler((thread, e) -> {
            errs[0] = e;
            countDown.countDown();
        });
        t.start();

        if (!countDown.await(6, TimeUnit.SECONDS)) {
            fail("execute table dump expire");
        }

        assertNull("errs[0] shall be null", errs[0]);

        EasyMock.verify(zkCoordinator);
    }

}
