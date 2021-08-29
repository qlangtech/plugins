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

import com.qlangtech.tis.datax.CuratorTaskMessage;
import com.qlangtech.tis.datax.DataXJobSingleProcessorExecutor;
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
    private static final ExecutorService dataXExecutor = newFixedThreadPool(10);// Executors.newCachedThreadPool();
    public static final String SYSTEM_KEY_LOGBACK_PATH_KEY = "logback.configurationFile";
    public static final String SYSTEM_KEY_LOGBACK_PATH_VALUE = "logback-assemble.xml";

    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(40),
                Executors.defaultThreadFactory());
    }

    static IRemoteJobTrigger getRemoteJobTrigger(IJoinTaskContext taskContext, LocalDataXJobSubmit localDataXJobSubmit, String dataXfileName) {
        // final JarLoader uberClassLoader = new TISJarLoader(pluginManager);

        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean success = new AtomicBoolean(false);
        return new IRemoteJobTrigger() {
            @Override
            public void submitJob() {
                dataXExecutor.submit(() -> {
                    try {
                        MDC.put(IParamContext.KEY_TASK_ID, String.valueOf(taskContext.getTaskId()));
                        MDC.put(TISCollectionUtils.KEY_COLLECTION, taskContext.getIndexName());

                        DataXJobSingleProcessorExecutor jobConsumer = new DataXJobSingleProcessorExecutor() {
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
                                        "-D" + SYSTEM_KEY_LOGBACK_PATH_KEY + "=" + SYSTEM_KEY_LOGBACK_PATH_VALUE
                                        , "-D" + CenterResource.KEY_notFetchFromCenterRepository + "=true"};
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

                        CuratorTaskMessage dataXJob = new CuratorTaskMessage();
                        dataXJob.setJobId(taskContext.getTaskId());
                        dataXJob.setJobName(dataXfileName);
                        dataXJob.setDataXName(taskContext.getIndexName());
                        jobConsumer.consumeMessage(dataXJob);
                        success.set(true);
                    } catch (Throwable e) {
                        //  e.printStackTrace();
                        logger.error("datax:" + taskContext.getIndexName() + ",jobName:" + dataXfileName, e);
                        success.set(false);
                        throw new RuntimeException(e);
                    } finally {
                        complete.set(true);
                    }
                });
            }

            @Override
            public RunningStatus getRunningStatus() {
                return new RunningStatus(0, complete.get(), success.get());
            }
        };
    }
}
