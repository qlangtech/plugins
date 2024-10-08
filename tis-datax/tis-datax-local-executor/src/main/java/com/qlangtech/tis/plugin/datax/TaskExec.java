/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSingleProcessorExecutor;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXJobSubmit.IDataXJobContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-22 16:21
 **/
public class TaskExec {
    private static final Logger logger = LoggerFactory.getLogger(TaskExec.class);


    static IRemoteTaskTrigger getRemoteJobTrigger(IDataXJobContext jobContext
            , LocalDataXJobSubmit localDataXJobSubmit, DataXJobInfo dataXJobInfo, IDataxProcessor processor
    ) {
        IJoinTaskContext taskContext = jobContext.getTaskContext();
        AtomicBoolean complete = new AtomicBoolean(false);
        return new IRemoteTaskTrigger() {
            DataXJobSingleProcessorExecutor<CuratorDataXTaskMessage> jobConsumer;
            boolean hasCanceled;

            @Override
            public void run() {
                try {

                    JobCommon.setMDC(taskContext);

                    jobConsumer = new DefaultDataxSplitTabSyncConsumer((IExecChainContext) taskContext, localDataXJobSubmit);

                    CuratorDataXTaskMessage
                            dataXJob = localDataXJobSubmit.getDataXJobDTO(jobContext, dataXJobInfo, processor);
                    jobConsumer.consumeMessage(dataXJob);

                } catch (Throwable e) {
                    final String datax = taskContext.hasIndexName() ? taskContext.getIndexName() : ("workflow:" + taskContext.getTaskId());
                    if (this.hasCanceled) {
                        logger.warn("datax:" + datax + " has been canceled");
                    } else {
                        logger.error("datax:" + datax + ",jobName:" + dataXJobInfo.jobFileName, e);
                        throw new RuntimeException(e);
                    }
                } finally {
                    complete.set(true);
                }
            }

            @Override
            public String getTaskName() {
                return dataXJobInfo.jobFileName;
            }

            @Override
            public void cancel() {
                if (jobConsumer == null) {
                    return;
                }
                jobConsumer.runningTask.forEach((taskId, watchdog) -> {
                    watchdog.destroyProcess();
                    logger.info("taskId:{} relevant task has been canceled", taskId);
                });
                this.hasCanceled = true;
            }
        };
    }
}
