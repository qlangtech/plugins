package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor;
import com.qlangtech.tis.datax.powerjob.impl.PowerJobTaskContext;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.powerjob.SelectedTabTriggersConfig;
import com.qlangtech.tis.test.TISEasyMock;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.lang3.tuple.Triple;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import tech.powerjob.worker.core.processor.TaskContext;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/21
 */
public class TestSplitTabSync implements TISEasyMock {

    @Test
    public void testExecSync() throws Exception {

        TaskContext tskContext = mock("tskContext", TaskContext.class);
        //http://192.168.28.200:7700/#/oms/wfInstanceDetail
        EasyMock.expect(tskContext.getJobParams()).andReturn("{ \"pre\":\"prep_totalpayinfo\", " + "\"post" +
                "\":\"hive_totalpayinfo_bind\", \"table\":\"totalpayinfo\", \"exec\":{ \"jobId\":-1, " +
                "\"execEpochMilli\":0, \"resType\":\"DataApp\", \"dataXName\":\"mysql_hive3\", " +
                "\"taskSerializeNum\":0, \"allRowsApproximately\":-1, \"jobInfo\":[ { " + "\"dataXInfo" +
                "\":\"totalpayinfo_0.json/order2/totalpayinfo\", \"taskSerializeNum\":0 } ] }, " + "\"dataxName" +
                "\":\"mysql_hive3\" }");
        EasyMock.expect(tskContext.getInstanceParams()).andReturn("{ \"app\":\"mysql_hive3\", \"dryRun\":false, " +
                "\"execTimeStamp\":1700553237897, \"taskid\":1924 }");


        this.replay();
        Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair =
                BasicTISTableDumpProcessor.createExecContext(new PowerJobTaskContext(tskContext), ExecPhase.Prepare);

        RpcServiceReference rpcSvc = StatusRpcClientFactory.getService(ITISCoordinator.create());
        pair.getLeft().setCoordinator(ITISCoordinator.create());
        // IDataxProcessor processor = DataxProcessor.load(null, pair.getRight().getDataXName());

        for (CuratorDataXTaskMessage tskMsg : pair.getRight().getSplitTabsCfg()) {
            SplitTabSync tabSync = new SplitTabSync(tskMsg);
            tabSync.execSync(pair.getLeft(), rpcSvc);
            this.verifyAll();
            return;
        }

        Assert.fail("have not process tskMsg ");


    }
}
