/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.TisZkClient;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.fullbuild.phasestatus.PhaseStatusCollection;
import com.qlangtech.tis.fullbuild.phasestatus.impl.DumpPhaseStatus;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.solrj.util.ZkUtils;
import com.tis.hadoop.rpc.ITISRpcService;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClient;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;

import java.io.File;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-21 16:39
 **/
public class TestLocalDataXJobSubmit extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
        HttpUtils.addMockGlobalParametersConfig();
        // Config.setNotFetchFromCenterRepository();
        CenterResource.setNotFetchFromCenterRepository();
    }

    public static final int TaskId = 1;
    public static final String dataXfileName = "customer_order_relation_0.json";
    public static final String dataXName = "baisuitestTestcase";
    public static final String statusCollectorHost = "127.0.0.1:3489";

    public void testCreateDataXJob() throws Exception {


        Optional<DataXJobSubmit> dataXJobSubmit = DataXJobSubmit.getDataXJobSubmit(DataXJobSubmit.InstanceType.LOCAL);
        Assert.assertTrue("dataXJobSubmit shall present", dataXJobSubmit.isPresent());

        LocalDataXJobSubmit jobSubmit = (LocalDataXJobSubmit) dataXJobSubmit.get();
        jobSubmit.setMainClassName(LocalDataXJobMainEntrypoint.class.getName());
        jobSubmit.setWorkingDirectory(new File("."));
        jobSubmit.setClasspath("target/classes:target/test-classes");

        AtomicReference<ITISRpcService> ref = new AtomicReference<>();
        ref.set(StatusRpcClient.AssembleSvcCompsite.MOCK_PRC);
        RpcServiceReference statusRpc = new RpcServiceReference(ref);

        IExecChainContext taskContext = EasyMock.createMock("taskContext", IExecChainContext.class);
        IDataxProcessor dataxProcessor = EasyMock.createMock("dataxProcessor", IDataxProcessor.class);
        EasyMock.expect(taskContext.getIndexName()).andReturn(dataXName).anyTimes();
        EasyMock.expect(taskContext.getTaskId()).andReturn(TaskId).anyTimes();

        int preSuccessTaskId = 99;
        PhaseStatusCollection preSuccessTask = new PhaseStatusCollection(preSuccessTaskId, new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.FullDump));
        DumpPhaseStatus preDumpStatus = new DumpPhaseStatus(preSuccessTaskId);
        DumpPhaseStatus.TableDumpStatus tableDumpStatus = preDumpStatus.getTable(dataXfileName);
        tableDumpStatus.setAllRows(LocalDataXJobMainEntrypoint.testAllRows);

        preSuccessTask.setDumpPhase(preDumpStatus);
        EasyMock.expect(taskContext.loadPhaseStatusFromLatest(dataXName)).andReturn(preSuccessTask).times(3);

        TisZkClient zkClient = EasyMock.createMock("TisZkClient", TisZkClient.class);

        String zkSubPath = "nodes0000000020";
        EasyMock.expect(zkClient.getChildren(
                ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH, null, true))
                .andReturn(Collections.singletonList(zkSubPath)).times(3);
        EasyMock.expect(zkClient.getData(EasyMock.eq(ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH + "/" + zkSubPath), EasyMock.isNull(), EasyMock.anyObject(Stat.class), EasyMock.eq(true)))
                .andReturn(statusCollectorHost.getBytes(TisUTF8.get())).times(3);

        EasyMock.expect(taskContext.getZkClient()).andReturn(zkClient).anyTimes();

        EasyMock.replay(taskContext, dataxProcessor, zkClient);
        IRemoteJobTrigger dataXJob = jobSubmit.createDataXJob(taskContext, statusRpc, dataxProcessor, dataXfileName);

        RunningStatus running = getRunningStatus(dataXJob);
        assertTrue("running.isSuccess", running.isSuccess());

        jobSubmit.setMainClassName(LocalDataXJobMainEntrypointThrowException.class.getName());
        dataXJob = jobSubmit.createDataXJob(taskContext, statusRpc, dataxProcessor, dataXfileName);

        running = getRunningStatus(dataXJob);
        assertFalse("shall faild", running.isSuccess());
        assertTrue("shall complete", running.isComplete());

        jobSubmit.setMainClassName(LocalDataXJobMainEntrypointCancellable.class.getName());
        dataXJob = jobSubmit.createDataXJob(taskContext, statusRpc, dataxProcessor, dataXfileName);
        running = getRunningStatus(dataXJob, false);
        Thread.sleep(2000);
        dataXJob.cancel();
        int i = 0;

        while (i++ < 3 && !(running = dataXJob.getRunningStatus()).isComplete()) {
            Thread.sleep(1000);
        }
        assertFalse("shall faild", running.isSuccess());
        assertTrue("shall complete", running.isComplete());

        EasyMock.verify(taskContext, dataxProcessor, zkClient);
    }

    protected RunningStatus getRunningStatus(IRemoteJobTrigger dataXJob) {
        return this.getRunningStatus(dataXJob, true);
    }

    protected RunningStatus getRunningStatus(IRemoteJobTrigger dataXJob, boolean waitting) {
        dataXJob.submitJob();
        RunningStatus running = null;
        while ((running = dataXJob.getRunningStatus()) != null && waitting) {
            if (running.isComplete()) {
                break;
            }
        }
        assertNotNull(running);
        return running;
    }
}
