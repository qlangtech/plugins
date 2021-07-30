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

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.plugin.BaiscPluginTest;
import com.tis.hadoop.rpc.ITISRpcService;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClient;
import org.easymock.EasyMock;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-21 16:39
 **/
public class TestLocalDataXJobSubmit extends BaiscPluginTest {
    public void testCreateDataXJob() {

        String dataXName = "baisuitestTestcase";

        Optional<DataXJobSubmit> dataXJobSubmit = DataXJobSubmit.getDataXJobSubmit(DataXJobSubmit.InstanceType.LOCAL);
        assertTrue("dataXJobSubmit shall present", dataXJobSubmit.isPresent());

        DataXJobSubmit jobSubmit = dataXJobSubmit.get();

        AtomicReference<ITISRpcService> ref = new AtomicReference<>();
        ref.set(StatusRpcClient.AssembleSvcCompsite.MOCK_PRC);
        RpcServiceReference statusRpc = new RpcServiceReference(ref);

        IJoinTaskContext taskContext = EasyMock.createMock("taskContext", IJoinTaskContext.class);
        IDataxProcessor dataxProcessor = EasyMock.createMock("dataxProcessor", IDataxProcessor.class);
        EasyMock.expect(taskContext.getIndexName()).andReturn(dataXName);
        EasyMock.expect(taskContext.getTaskId()).andReturn(1);
        File cfgDir = new File(Config.getMetaCfgDir(), TIS.KEY_TIS_PLUGIN_CONFIG + "/ap/" + dataXName + "/" + DataxProcessor.DATAX_CFG_DIR_NAME);
        assertTrue("cfgDir.exists:" + cfgDir.getAbsolutePath(), cfgDir.exists());

        EasyMock.expect(dataxProcessor.getDataxCfgDir(null)).andReturn(cfgDir);
        String dataXfileName = "customer_order_relation_0.json";

//        IJoinTaskContext taskContext
//            , RpcServiceReference statusRpc, IDataxProcessor dataxProcessor, String dataXfileName

        EasyMock.replay(taskContext, dataxProcessor);
        IRemoteJobTrigger dataXJob = jobSubmit.createDataXJob(taskContext, statusRpc, dataxProcessor, dataXfileName);

        dataXJob.submitJob();
        RunningStatus running = null;
        while ((running = dataXJob.getRunningStatus()) != null) {
            if (running.isComplete()) {
                break;
            }
        }

        assertNotNull(running);
        assertTrue("running.isSuccess", running.isSuccess());

        EasyMock.verify(taskContext, dataxProcessor);
    }
}
