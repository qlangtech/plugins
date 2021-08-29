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
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
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

        TisZkClient zkClient = EasyMock.createMock("TisZkClient", TisZkClient.class);

        String zkSubPath = "nodes0000000020";
        EasyMock.expect(zkClient.getChildren(
                ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH, null, true))
                .andReturn(Collections.singletonList(zkSubPath)).times(2);
        EasyMock.expect(zkClient.getData(EasyMock.eq(ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH + "/" + zkSubPath), EasyMock.isNull(), EasyMock.anyObject(Stat.class), EasyMock.eq(true)))
                .andReturn(statusCollectorHost.getBytes(TisUTF8.get())).times(2);

        EasyMock.expect(taskContext.getZkClient()).andReturn(zkClient).anyTimes();


        // File cfgDir = new File(Config.getMetaCfgDir(), TIS.KEY_TIS_PLUGIN_CONFIG + "/ap/" + dataXName + "/" + DataxProcessor.DATAX_CFG_DIR_NAME);
        // Assert.assertTrue("cfgDir.exists:" + cfgDir.getAbsolutePath(), cfgDir.exists());

        // EasyMock.expect(dataxProcessor.getDataxCfgDir(null)).andReturn(cfgDir);


//        IJoinTaskContext taskContext
//            , RpcServiceReference statusRpc, IDataxProcessor dataxProcessor, String dataXfileName

        EasyMock.replay(taskContext, dataxProcessor, zkClient);
        IRemoteJobTrigger dataXJob = jobSubmit.createDataXJob(taskContext, statusRpc, dataxProcessor, dataXfileName);

        RunningStatus running = getRunningStatus(dataXJob);
        assertTrue("running.isSuccess", running.isSuccess());

        jobSubmit.setMainClassName(LocalDataXJobMainEntrypointThrowException.class.getName());
        dataXJob = jobSubmit.createDataXJob(taskContext, statusRpc, dataxProcessor, dataXfileName);

        running = getRunningStatus(dataXJob);
        assertFalse("shall faild", running.isSuccess());


        EasyMock.verify(taskContext, dataxProcessor, zkClient);
    }

    protected RunningStatus getRunningStatus(IRemoteJobTrigger dataXJob) {
        dataXJob.submitJob();
        RunningStatus running = null;
        while ((running = dataXJob.getRunningStatus()) != null) {
            if (running.isComplete()) {
                break;
            }
        }
        assertNotNull(running);
        return running;
    }
}
