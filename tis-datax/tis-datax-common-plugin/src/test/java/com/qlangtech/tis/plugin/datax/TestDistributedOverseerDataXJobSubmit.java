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

import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.manage.impl.DataFlowAppSource;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.tis.hadoop.rpc.ITISRpcService;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClient;
import junit.framework.TestCase;
import org.easymock.EasyMock;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-07 13:58
 **/
public class TestDistributedOverseerDataXJobSubmit extends TestCase {

    public static final String DATAX_NAME = "baisuitest";
    public static final Integer DATAX_TASK_ID = 123;
    public static final String DATAX_JOB_FILE_NAME = "customer_order_relation_1.json";

    public void testPushMsgToDistributeQueue() {

        DataXJobWorker dataxJobWorker = DataXJobWorker.getJobWorker(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
        assertEquals("/datax/jobs", dataxJobWorker.getZkQueuePath());
        assertEquals("192.168.28.200:2181/tis/cloud", dataxJobWorker.getZookeeperAddress());


        DataxProcessor dataxProcessor = IAppSource.load(DATAX_NAME);
        assertNotNull(dataxProcessor);

        //IDataxProcessor dataxProcessor = EasyMock.createMock("dataxProcessor", IDataxProcessor.class);
        // EasyMock.expect(dataxProcessor.getDataxCfgDir()).andReturn();

        IJoinTaskContext taskContext = EasyMock.createMock("joinTaskContext", IJoinTaskContext.class);


        EasyMock.expect(taskContext.getIndexName()).andReturn(DATAX_NAME);
        EasyMock.expect(taskContext.getTaskId()).andReturn(DATAX_TASK_ID);

        AtomicReference<ITISRpcService> ref = new AtomicReference<>();
        ref.set(StatusRpcClient.AssembleSvcCompsite.MOCK_PRC);
        RpcServiceReference svcRef = new RpcServiceReference(ref);

        Optional<DataXJobSubmit> jobSubmit = DataXJobSubmit.getDataXJobSubmit(DataXJobSubmit.InstanceType.DISTRIBUTE);
        assertTrue(jobSubmit.isPresent());
        DataXJobSubmit submit = jobSubmit.get();


        EasyMock.replay(taskContext);
        //IJoinTaskContext taskContext
        //            , RpcServiceReference statusRpc, IDataxProcessor dataxProcessor, String dataXfileName
        IRemoteJobTrigger dataXJob = submit.createDataXJob(taskContext, svcRef, dataxProcessor, DATAX_JOB_FILE_NAME);
        dataXJob.submitJob();
        EasyMock.verify(taskContext);
    }
}
