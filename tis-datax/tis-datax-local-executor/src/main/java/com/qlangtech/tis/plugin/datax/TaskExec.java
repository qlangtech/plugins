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
import com.qlangtech.tis.datax.DataXJobSingleProcessorExecutor;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TISCollectionUtils;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.solrj.util.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-22 16:21
 **/
public class TaskExec {
    private static final Logger logger = LoggerFactory.getLogger(TaskExec.class);


    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(200),
                Executors.defaultThreadFactory());
    }

    static IRemoteJobTrigger getRemoteJobTrigger(IJoinTaskContext taskContext, LocalDataXJobSubmit localDataXJobSubmit, String dataXfileName) {
        // final JarLoader uberClassLoader = new TISJarLoader(pluginManager);

        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean success = new AtomicBoolean(false);
        return new IRemoteJobTrigger() {
            DataXJobSingleProcessorExecutor jobConsumer;
            boolean hasCanceled;
            private final ExecutorService dataXExecutor = newFixedThreadPool(2);// Executors.newCachedThreadPool();

            @Override
            public void submitJob() {
                dataXExecutor.submit(() -> {
                    try {
                        MDC.put(IParamContext.KEY_TASK_ID, String.valueOf(taskContext.getTaskId()));
                        MDC.put(TISCollectionUtils.KEY_COLLECTION, taskContext.getIndexName());

                        jobConsumer = new DataXJobSingleProcessorExecutor() {
                            @Override
                            protected DataXJobSubmit.InstanceType getExecMode() {
                                return DataXJobSubmit.InstanceType.LOCAL;
                            }

                            @Override
                            protected String getClasspath() {
                                return localDataXJobSubmit.getClasspath();
                            }

                            @Override
                            protected boolean useRuntimePropEnvProps() {
                                return false;
                            }

                            @Override
                            protected String[] getExtraJavaSystemPrams() {
                                return new String[]{
                                        // "-D" + SYSTEM_KEY_LOGBACK_PATH_KEY + "=" + SYSTEM_KEY_LOGBACK_PATH_VALUE
                                        "-D" + CenterResource.KEY_notFetchFromCenterRepository + "=true"};
                            }

                            @Override
                            protected String getIncrStateCollectAddress() {
                                return ZkUtils.getFirstChildValue(
                                        ((IExecChainContext) taskContext).getZkClient(), ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH);
                            }

                            @Override
                            protected String getMainClassName() {
                                return localDataXJobSubmit.getMainClassName();
                            }

                            @Override
                            protected File getWorkingDirectory() {
                                return localDataXJobSubmit.getWorkingDirectory();
                            }
                        };

                        CuratorDataXTaskMessage dataXJob = localDataXJobSubmit.getDataXJobDTO(taskContext, dataXfileName);

//                        new CuratorDataXTaskMessage();
//                        dataXJob.setJobId(taskContext.getTaskId());
//                        dataXJob.setJobName(dataXfileName);
//                        dataXJob.setDataXName(taskContext.getIndexName());
                        jobConsumer.consumeMessage(dataXJob);
                        success.set(true);
                    } catch (Throwable e) {
                        //  e.printStackTrace();
                        success.set(false);
                        if (this.hasCanceled) {
                            logger.warn("datax:" + taskContext.getIndexName() + " has been canceled");
                        } else {
                            logger.error("datax:" + taskContext.getIndexName() + ",jobName:" + dataXfileName, e);
                            throw new RuntimeException(e);
                        }
                    } finally {
                        complete.set(true);
                        shutdownExecutor();
                    }
                });
            }

            private void shutdownExecutor() {
                try {
                    dataXExecutor.shutdownNow();
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                }
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
                shutdownExecutor();
                this.hasCanceled = true;
            }

            @Override
            public RunningStatus getRunningStatus() {
                return new RunningStatus(0, complete.get(), success.get());
            }
        };
    }
}
