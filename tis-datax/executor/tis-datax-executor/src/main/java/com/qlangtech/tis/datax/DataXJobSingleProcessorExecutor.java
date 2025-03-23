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

package com.qlangtech.tis.datax;

import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.web.start.TisAppLaunch;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 独立进程中执行DataX任务，这样可以有效避免每次执行DataX任务由于ClassLoader的冲突导致的错误
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-08-29 10:19
 **/
public abstract class DataXJobSingleProcessorExecutor<T extends IDataXTaskRelevant> {
    private static final Logger logger = LoggerFactory.getLogger(DataXJobSingleProcessorExecutor.class);

    // 记录当前正在执行的任务<taskid,ExecuteWatchdog>
    public final ConcurrentHashMap<Integer, ExecuteWatchdog> runningTask = new ConcurrentHashMap<>();

    // @Override
    public void consumeMessage(T msg) throws Exception {
        //MDC.put();
        Integer taskId = msg.getTaskId();
        String jobName = msg.getJobName();
        String dataxName = msg.getDataXName();
        //  StoreResourceType resType = Objects.requireNonNull(msg.getResType(), "resType can not be null");
        //        MDC.put(JobCommon.KEY_TASK_ID, String.valueOf(jobId));
        //        MDC.put(JobCommon.KEY_COLLECTION, dataxName);
        JobCommon.setMDC(taskId, dataxName);


        // 查看当前任务是否正在进行中，如果已经终止则要退出
        execSystemTask(msg, taskId, jobName, dataxName);
    }

    protected void execSystemTask(T msg, Integer jobId, String jobName, String dataxName) throws IOException,
            InterruptedException, DataXJobSingleProcessorException {
        if (!isCurrentJobProcessing(jobId)) {
            logger.warn("current job id:{} jobName:{}, dataxName:{} is not processing skipping!!", jobId, jobName,
                    dataxName);
            return;
        }

        DataXJobSubmitParams jobSubmitParams = DataXJobSubmitParams.getDftIfEmpty();

        jobSubmitParams.execParallelTask(dataxName, () -> {
            // 在VM中控制进入可用区执行的实例数量
            {

                CommandLine cmdLine = new CommandLine("java");
                cmdLine.addArgument("-D" + Config.KEY_DATA_DIR + "=" + Config.getDataDir().getAbsolutePath());
                cmdLine.addArgument("-D" + Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS + "=" + this.useRuntimePropEnvProps());
                cmdLine.addArgument("-D" + TisAppLaunch.KEY_LOG_DIR + "=" + TisAppLaunch.getLogDir().getAbsolutePath());
                cmdLine.addArgument("-D" + Config.KEY_RUNTIME + "=daily");
                cmdLine.addArgument("-D" + Config.SYSTEM_KEY_LOGBACK_PATH_KEY + "=" + Config.SYSTEM_KEY_LOGBACK_PATH_VALUE);
                cmdLine.addArgument("-D" + DataxUtils.EXEC_TIMESTAMP + "=" + msg.getExecEpochMilli());
                cmdLine.addArgument("-Dfile.encoding=" + TisUTF8.getName());
                //  cmdLine.addArgument("-D" + Config.KEY_ASSEMBLE_HOST + "=" + Config.getAssembleHost());
                File localLoggerPath = null;
                if ((localLoggerPath = msg.getSpecifiedLocalLoggerPath()) != null) {
                    cmdLine.addArgument("-D" + Config.EXEC_LOCAL_LOGGER_FILE_PATH + "=" + localLoggerPath.getAbsolutePath());
                }
                for (String sysParam : this.getExtraJavaSystemPrams()) {
                    cmdLine.addArgument(sysParam, false);
                }

                cmdLine.addArgument("-classpath");
                cmdLine.addArgument(getClasspath());
                cmdLine.addArgument(getMainClassName());
                addMainClassParams(msg, jobId, jobName, dataxName, cmdLine);

                DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

                ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);

                DefaultExecutor executor = new DefaultExecutor();
                File workingDir = getWorkingDirectory();
                executor.setWorkingDirectory(workingDir);

                executor.setStreamHandler(new PumpStreamHandler(System.out));
                executor.setExitValue(0);
                executor.setWatchdog(watchdog);
                String command = Arrays.stream(cmdLine.toStrings()).collect(Collectors.joining(" "));
                logger.info("workDir:{},command:{}", workingDir.getAbsolutePath(), command);
                if (DataxUtils.localDataXCommandConsumer != null) {
                    DataxUtils.localDataXCommandConsumer.accept(command);
                }
                executor.execute(cmdLine, resultHandler);

                runningTask.computeIfAbsent(jobId, (id) -> executor.getWatchdog());

                logger.info("waitForTerminator jobId:{},dataxName:{},taskExpireHours:{}", jobId, dataxName, jobSubmitParams.taskExpireHours);
                waitForTerminator(jobId, dataxName, jobSubmitParams.taskExpireHours, resultHandler);

            }
        });

    }

    /**
     * 等待任务执行结束
     *
     * @param jobId
     * @param dataxName
     * @param resultHandler
     * @throws InterruptedException
     * @throws DataXJobSingleProcessorException
     */
    protected void waitForTerminator(Integer jobId, String dataxName, final Integer taskExpireHours, DefaultExecuteResultHandler resultHandler) throws InterruptedException, DataXJobSingleProcessorException {

        int timeout = Objects.requireNonNull(taskExpireHours, "taskExpireHours can not be null");
        try {
            // 等待5个小时
            resultHandler.waitFor(TimeUnit.HOURS.toMillis(timeout));

            if (resultHandler.getExitValue() != DataXJobInfo.DATAX_THREAD_PROCESSING_CANCAL_EXITCODE) {
                if ( //resultHandler.hasResult() &&
                        resultHandler.getExitValue() != 0) {
                    // it was killed on purpose by the watchdog
                    if (resultHandler.getException() != null) {
                        logger.error("dataX:" + dataxName + ",ERROR MSG:" + resultHandler.getException().getMessage(), resultHandler.getException());
                        // throw new RuntimeException(command, resultHandler.getException());
                        throw new DataXJobSingleProcessorException("dataX:" + dataxName + ",ERROR MSG:" + resultHandler.getException().getMessage());
                    }
                }

                if (!resultHandler.hasResult()) {
                    // 此处应该是超时了
                    throw new DataXJobSingleProcessorException("dataX:" + dataxName + ",job execute timeout,wait "
                            + "for " + timeout + " hours");
                }
            }
        } finally {
            runningTask.remove(jobId);
        }
    }


    protected abstract void addMainClassParams(T msg, Integer taskId, String jobName, String dataxName,
                                               CommandLine cmdLine);


    protected boolean isCurrentJobProcessing(Integer jobId) {
        return true;
    }

    protected abstract DataXJobSubmit.InstanceType getExecMode();

    protected abstract String getClasspath();

    protected boolean useRuntimePropEnvProps() {
        return true;
    }

    protected String[] getExtraJavaSystemPrams() {
        return new String[0];
    }

    /**
     * @return
     */
    protected abstract String getMainClassName();

    /**
     * @return
     */
    protected abstract File getWorkingDirectory();


    /**
     * Assemble 日志收集器地址
     *
     * @return
     */
    protected abstract String getIncrStateCollectAddress();

    //    @Override
    //    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
    //        logger.warn("curator stateChanged to new Status:" + connectionState);
    //    }
}
