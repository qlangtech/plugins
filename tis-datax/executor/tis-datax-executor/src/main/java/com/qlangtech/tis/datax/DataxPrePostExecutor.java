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


import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.exec.impl.DataXPipelineExecContext;
import com.qlangtech.tis.exec.impl.WorkflowExecContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * DataX 生命周期前后钩子 执行
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2021-04-20 12:38
 */
public class DataxPrePostExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DataxPrePostExecutor.class);


    /**
     * @param args
     * @see DataxPrePostConsumer
     * @see DataXJobSingleProcessorExecutor
     * 入口开始执行
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            throw new IllegalArgumentException("args length must be 8,but now is " + String.join(",", args));
        }
        Integer jobId = Integer.parseInt(args[0]);
        String dataXName = args[1];
        String incrStateCollectAddress = args[2];
        if (StringUtils.isEmpty(incrStateCollectAddress)) {
            throw new IllegalArgumentException("arg 'incrStateCollectAddress' can not be null");
        }

        boolean isDisableGrpcRemoteServerConnect = Boolean.parseBoolean(args[8]);
        RpcServiceReference statusRpc =
                StatusRpcClientFactory.getService(ITISCoordinator.create(!isDisableGrpcRemoteServerConnect, Optional.of(incrStateCollectAddress)));
        AssembleSvcCompsite.statusRpc = (statusRpc);

        final String lifecycleHookName = args[3];
        // final ISelectedTab tab = new DefaultTab(args[4]);
        final String entity = (args[4]);
        if (StringUtils.isEmpty(entity)) {
            throw new IllegalStateException("param table name can not be empty");
        }

        StoreResourceType resType = StoreResourceType.parse(args[5]);
        String jobName = args[6];
        if (StringUtils.isEmpty(jobName)) {
            throw new IllegalArgumentException("param jobName can not be null");
        }
        final long execEpochMilli = Long.parseLong(args[7]);


        JobCommon.setMDC(jobId, dataXName);

        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("arg 'dataXName' can not be null");
        }


        //  IRemoteTaskTrigger hookTrigger = null;
        try {
            IDataxProcessor dataxProcessor = DataxProcessor.load(null, resType, dataXName);
            IDataxReader reader = dataxProcessor.getReader(null);
            ISelectedTab tab = reader.getSelectedTab(entity);
            IDataXBatchPost batchPost =
                    IDataxWriter.castBatchPost(Objects.requireNonNull(dataxProcessor.getWriter(null), "dataXName" +
                            ":" + dataXName + " relevant dataXWriter can not be null"));
            //  final EntityName tabEntity = batchPost.parseEntity(tab);

            final AbstractExecContext execContext = createExecContext(execEpochMilli, dataxProcessor);

            // execContext.setResType(resType);

            IDataXBatchPost.process(dataxProcessor, tab, (batchPostTask, entryName) -> {
                IRemoteTaskTrigger hookTrigger = null;
                if (IDataXBatchPost.KEY_POST.equalsIgnoreCase(lifecycleHookName)) {
                    hookTrigger = batchPost.createPostTask(
                            execContext, entryName, tab, dataxProcessor.getDataxCfgFileNames(null, Optional.empty()));
                } else if (IDataXBatchPost.KEY_PREP.equalsIgnoreCase(lifecycleHookName)) {
                    hookTrigger = batchPost.createPreExecuteTask(execContext, entryName, tab);
                } else {
                    throw new IllegalArgumentException("illegal lifecycleHookName:" + lifecycleHookName);
                }
                if (!StringUtils.equals(hookTrigger.getTaskName(), jobName)) {
                    logger.warn("hookTrigger.getTaskName:{} is not equal with jobName:{}", hookTrigger.getTaskName(),
                            jobName);
                }
                Objects.requireNonNull(hookTrigger, "hookTrigger can not be null");
                hookTrigger.run();
                return null;
            });


//            if (IDataXBatchPost.KEY_POST.equalsIgnoreCase(lifecycleHookName)) {
//                hookTrigger = batchPost.createPostTask(
//                        execContext, entity, tab, dataxProcessor.getDataxCfgFileNames(null, Optional.empty()));
//            } else if (IDataXBatchPost.KEY_PREP.equalsIgnoreCase(lifecycleHookName)) {
//                hookTrigger = batchPost.createPreExecuteTask(execContext, entity, tab);
//            } else {
//                throw new IllegalArgumentException("illegal lifecycleHookName:" + lifecycleHookName);
//            }
//            if (!StringUtils.equals(hookTrigger.getTaskName(), jobName)) {
//                logger.warn("hookTrigger.getTaskName:{} is not equal with jobName:{}", hookTrigger.getTaskName(),
//                        jobName);
//            }
//            Objects.requireNonNull(hookTrigger, "hookTrigger can not be null");
//            hookTrigger.run();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            try {

                try {
                    if (statusRpc != null) {
//                        StatusRpcClientFactory.AssembleSvcCompsite svc =
//                                (StatusRpcClientFactory.AssembleSvcCompsite) statusRpc.get();
                        if (IDataXBatchPost.KEY_POST.equalsIgnoreCase(lifecycleHookName)) {
                            JoinPhaseStatus.JoinTaskStatus joinStatus = new JoinPhaseStatus.JoinTaskStatus(jobName);
                            joinStatus.setFaild(true);
                            joinStatus.setComplete(true);
                            joinStatus.setStart();
                            statusRpc.reportJoinStatus(jobId, joinStatus);
                        } else if (IDataXBatchPost.KEY_PREP.equalsIgnoreCase(lifecycleHookName)) {
                            statusRpc.reportDumpJobStatus(true, true, false, jobId, jobName, -1, -1);
                        } else {
                            throw new IllegalArgumentException("illegal lifecycleHookName:" + lifecycleHookName);
                        }
                    }
                } catch (Throwable ex) {
                    // throw new RuntimeException(ex);
                    logger.warn(e.getMessage(), e);
                }


                //确保日志向远端写入了
                Thread.sleep(3000);
            } catch (InterruptedException ex) {

            }

            System.exit(1);
            return;
        } finally {
            //            try {
            //              //  statusRpc.close();
            //            } catch (Throwable e) {
            //            }
        }
        logger.info("dataX:" + dataXName + ",taskid:" + jobId + " finished");
        System.exit(0);
    }

    private static AbstractExecContext createExecContext(long execEpochMilli, IDataxProcessor dataxProcessor) {
        AbstractExecContext execContext;
        StoreResourceType resType = dataxProcessor.getResType();
        String dataXName = dataxProcessor.identityValue();
        switch (resType) {
            case DataApp:
                execContext = new DataXPipelineExecContext(dataXName, execEpochMilli) {
                    @Override
                    public IDataxProcessor getProcessor() {
                        return dataxProcessor;
                    }
                };
                break;
            case DataFlow:
                execContext = new WorkflowExecContext(null, execEpochMilli) {
                    @Override
                    public IDataxProcessor getProcessor() {
                        return dataxProcessor;
                    }
                };
                break;
            default:
                throw new IllegalStateException("illegal resType:" + resType);
        }
        return execContext;
    }


}
