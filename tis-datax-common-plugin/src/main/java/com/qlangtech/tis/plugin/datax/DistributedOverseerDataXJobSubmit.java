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

import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobConsumer;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.fullbuild.indexbuild.impl.AsynRemoteJobTrigger;
import com.qlangtech.tis.order.center.IAppSourcePipelineController;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 21:41
 **/
@TISExtension()
public class DistributedOverseerDataXJobSubmit extends DataXJobSubmit {

    public DistributedOverseerDataXJobSubmit() {

    }

    @Override
    public InstanceType getType() {
        return InstanceType.DISTRIBUTE;
    }

    @Override
    public IRemoteJobTrigger createDataXJob(IJoinTaskContext taskContext, RpcServiceReference statusRpc, IDataxProcessor dataxProcessor, String dataXfileName) {
        IAppSourcePipelineController pipelineController = taskContext.getPipelineController();
        DistributedQueue<CuratorDataXTaskMessage> distributedQueue = getCuratorDistributedQueue();
        // File jobPath = new File(dataxProcessor.getDataxCfgDir(null), dataXfileName);
        return new AsynRemoteJobTrigger(dataXfileName) {
            @Override
            public void submitJob() {
                try {
                    CuratorDataXTaskMessage msg = getDataXJobDTO(taskContext, dataXfileName);
                    distributedQueue.put(msg);
                    pipelineController.registerAppSubExecNodeMetrixStatus(
                            IAppSourcePipelineController.DATAX_FULL_PIPELINE + taskContext.getIndexName(), dataXfileName);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RunningStatus getRunningStatus() {
                return RunningStatus.SUCCESS;
            }

            @Override
            public void cancel() {
                pipelineController.stop(
                        IAppSourcePipelineController.DATAX_FULL_PIPELINE + taskContext.getIndexName());
            }
        };
    }


    private CuratorFramework curatorClient = null;
    private DistributedQueue<CuratorDataXTaskMessage> curatorDistributedQueue = null;

    private DistributedQueue<CuratorDataXTaskMessage> getCuratorDistributedQueue() {
        synchronized (this) {
            if (curatorClient != null && !curatorClient.getZookeeperClient().isConnected()) {
                curatorClient.close();
                curatorClient = null;
                curatorDistributedQueue = null;
            }
            if (curatorDistributedQueue == null) {
                DataXJobWorker dataxJobWorker = DataXJobWorker.getDataxJobWorker();
                if (curatorClient == null) {
                    this.curatorClient = DataXJobConsumer.getCuratorFramework(dataxJobWorker.getZookeeperAddress());
                }
                this.curatorDistributedQueue = DataXJobConsumer.createQueue(curatorClient, dataxJobWorker.getZkQueuePath(), null);
            }
            return this.curatorDistributedQueue;
        }
    }
}
