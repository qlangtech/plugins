package com.qlangtech.tis.offline;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.cloud.MockZKUtils;
import com.qlangtech.tis.dump.LocalTableDumpFactory;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.MockTaskContextUtils;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.fullbuild.indexbuild.TaskContext;
import com.qlangtech.tis.indexbuild.LocalIndexBuilderTriggerFactory;
import com.qlangtech.tis.manage.common.ConfigFileReader;
import com.qlangtech.tis.manage.common.SnapshotDomain;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.order.dump.task.ITableDumpConstant;
import com.qlangtech.tis.order.dump.task.ITestDumpCommon;
import com.qlangtech.tis.order.dump.task.MockDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.test.TISTestCase;
import com.qlangtech.tis.trigger.jst.ImportDataProcessInfo;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-03-03 13:52
 **/
public class TestLocalTableDumpFactory extends TISTestCase implements ITestDumpCommon {


    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.clearMocks();
        FileUtils.deleteQuietly(LocalTableDumpFactory.getLocalOfflineRootDir());
    }

    public void testSingleTableDump() throws Exception {

        LocalTableDumpFactory tableDumpFactory = new LocalTableDumpFactory();
        File dumpRoot = LocalTableDumpFactory.getLocalOfflineRootDir();

        tableDumpFactory.name = "test";

        DataSourceFactory mockEmployeesDataSource = MockDataSourceFactory.getMockEmployeesDataSource();

        tableDumpFactory.setDataSourceFactoryGetter((tab) -> {
            return mockEmployeesDataSource;
        });

        ITISCoordinator zkCoordinator = MockZKUtils.createZkMock();
        IJoinTaskContext execContext = this.mock("execContext", IJoinTaskContext.class);
        //EntityName targetTableName = EntityName.parse(DB_EMPLOYEES+"."); ctx.getAttribute(IParamContext.KEY_BUILD_TARGET_TABLE_NAME);
        EasyMock.expect(execContext.getAttribute(IParamContext.KEY_BUILD_TARGET_TABLE_NAME)).andReturn(getEmployeeTab()).anyTimes();
        replay();
        int round = 0;
        ArrayDeque<Date> createDates = Queues.newArrayDeque();
        // 一共测试5轮
        Date timestamp = null;
        while (round++ < 5) {
            timestamp = new Date();
            createDates.addLast(timestamp);
            TaskContext taskContext = MockTaskContextUtils.create(timestamp);
            taskContext.setCoordinator(zkCoordinator);
            /** -----------------------------------------------------------
             * 开始执行数据导入流程
             -----------------------------------------------------------*/
            startDump(tableDumpFactory, taskContext);

            /** -----------------------------------------------------------
             * 开始执行索引构建流程
             -----------------------------------------------------------*/
            startIndexBuild(execContext, zkCoordinator, MockTaskContextUtils.timeFormatYyyyMMddHHmmss.get().format(timestamp));
            Thread.sleep(1000);
        }
        int index = 0;
        File tableRoot = new File(dumpRoot, DB_EMPLOYEES + "/" + TABLE_EMPLOYEES + "/all");
        assertTrue(tableRoot.exists());
        String[] subTimeStampFiles = tableRoot.list();
        Set<String> timestamps = Sets.newHashSet();
        int maxHistorySave = ITableDumpConstant.MAX_PARTITION_SAVE + 1;
        while (index++ < maxHistorySave) {
            timestamps.add(MockTaskContextUtils.timeFormatYyyyMMddHHmmss.get().format(createDates.pollLast()));
        }
        assertEquals("maxHistorySave", maxHistorySave, subTimeStampFiles.length);
        for (String subFile : subTimeStampFiles) {
            assertTrue("shall contain file:" + new File(tableRoot, subFile), timestamps.contains(subFile));
            //TODO 继续校验文件夹中的内容是否正确
        }

        File indexBuildRoot = new File(dumpRoot, INDEX_COLLECTION + "/all/0/output");
        for (String indexBuildRootSub : indexBuildRoot.list()) {
            assertTrue("shall contain file:" + new File(indexBuildRoot, indexBuildRootSub), timestamps.contains(indexBuildRootSub));
        }

        verifyAll();
    }


    public void startIndexBuild(IJoinTaskContext execContext, ITISCoordinator zkCoordinator, String timePoint) throws Exception {
        LocalIndexBuilderTriggerFactory builderTriggerFactory = new LocalIndexBuilderTriggerFactory();

        File localOfflineDir = LocalTableDumpFactory.getLocalOfflineRootDir();


        String indexName = ITestDumpCommon.INDEX_COLLECTION;
        String groupNum = "0";
        Integer taskId = 123;
        ITISFileSystem fileSystem = builderTriggerFactory.getFileSystem();

        ImportDataProcessInfo buildParam = new ImportDataProcessInfo(taskId, fileSystem, zkCoordinator);

        buildParam.setIndexName(indexName);
        MockDataSourceFactory employeesDataSource = MockDataSourceFactory.getMockEmployeesDataSource();

        List<ColumnMetaData> eployeeTableMeta = employeesDataSource.getTableMetadata(TABLE_EMPLOYEES);
        String colsLiteria = eployeeTableMeta.stream().map((c) -> c.getKey()).collect(Collectors.joining(","));
        buildParam.setBuildTableTitleItems(colsLiteria);


        SnapshotDomain snapshot = com.qlangtech.tis.manage.common.SnapshotDomainUtils.mockEmployeeSnapshotDomain();
        snapshot.writeResource2fs(fileSystem, buildParam.getCoreName(Integer.parseInt(groupNum)), ConfigFileReader.FILE_SCHEMA);
        snapshot.writeResource2fs(fileSystem, buildParam.getCoreName(Integer.parseInt(groupNum)), ConfigFileReader.FILE_SOLR);

        IRemoteJobTrigger buildJob = builderTriggerFactory.createBuildJob(execContext, timePoint, indexName, groupNum, buildParam);
        buildJob.submitJob();
        /** -----------------------------------------------------------
         * 开始执行索引build
         -----------------------------------------------------------*/
        TestLocalTableDumpFactory.waitJobTerminatorAndAssert(buildJob);

    }


    /**
     * 单轮 dump测试
     *
     * @param tableDumpFactory
     * @param taskContext
     * @throws Exception
     */
    private void startDump(LocalTableDumpFactory tableDumpFactory, TaskContext taskContext) throws Exception {
        IRemoteJobTrigger singleTableDumpJob = tableDumpFactory.createSingleTableDumpJob(this.getEmployeeTab(), taskContext);


        singleTableDumpJob.submitJob();

        waitJobTerminatorAndAssert(singleTableDumpJob);
    }

    public static void waitJobTerminatorAndAssert(IRemoteJobTrigger triggerJob) throws InterruptedException {
        final RunningStatus runningStatus = triggerJob.getRunningStatus();
        assertNotNull(runningStatus);
        CountDownLatch countDown = new CountDownLatch(1);
        Runnable waitThread = () -> {
            try {
                RunningStatus s = runningStatus;
                while (!s.isComplete()) {
                    Thread.sleep(1000);
                    s = triggerJob.getRunningStatus();
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

        if (!countDown.await(600, TimeUnit.SECONDS)) {
            fail("execute table dump expire 6s");
        }

        assertNull("errs[0] shall be null", errs[0]);
    }

}
