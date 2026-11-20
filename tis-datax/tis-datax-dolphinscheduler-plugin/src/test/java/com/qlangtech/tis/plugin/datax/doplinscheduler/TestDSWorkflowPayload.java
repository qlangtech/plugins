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

package com.qlangtech.tis.plugin.datax.doplinscheduler;

import com.alibaba.fastjson.JSONArray;
import com.qlangtech.tis.assemble.TriggerType;
import com.qlangtech.tis.coredefine.module.action.DistributeJobTriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.biz.dal.pojo.ApplicationCriteria;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.CreateNewTaskResult;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.datax.SPIExecContext;
import com.qlangtech.tis.plugin.datax.WorkflowSPIInitializer;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerEndpoint;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.ExportTISPipelineToDolphinscheduler;
import com.qlangtech.tis.realtime.yarn.rpc.SynResTarget;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.mockito.Mockito;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.Optional;

import org.junit.Test;
/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 22:29
 **/
public class TestDSWorkflowPayload {

    private static final String appName = "mysql";

    private static final long dsWorkflowId = 118072866846144l;

    private static final String exceptAppPayloadContent = "{\n" +
            "\t\"workflow_id\":" + dsWorkflowId + ",\n" +
            "\t\"execRange\":[\n" +
            "\t\t\"dump\",\n" +
            "\t\t\"dump\"\n" +
            "\t]\n" +
            "}";

    private static DolphinSchedulerEndpoint dsEndpoint;

    private ICommonDAOContext commonDAOContext;
    private IApplicationDAO applicationDAO;
    private IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO;

    static {
        dsEndpoint = new DolphinSchedulerEndpoint();
        dsEndpoint.serverPath = "http://192.168.28.201:12345/dolphinscheduler";
        dsEndpoint.serverToken = "f02d2883118664579ea32a659b2c9652";
        dsEndpoint.name = "test";
    }


//    public void testScheduler() throws Exception {
//        ExecutorService scheduledExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
//            @Override
//            public Thread newThread(Runnable r) {
//                Thread t = new Thread(r);
//                t.setUncaughtExceptionHandler((thread, exception) -> {
//                    System.out.println("================================");
//                    exception.printStackTrace();
//                    //  logger.error(exception.getMessage(), exception);
//                });
//                return t;
//            }
//        });
//
//        scheduledExecutorService.execute(() -> {
//            throw new IllegalArgumentException("xxxxxxxxxxxxx");
//        });
//
//
//
//        Thread.sleep(990000);
//    }
//
//    public void testXXX(){
//        System.out.println( (new JSONArray()).toJSONString());
//    }

    @Test
    public void testTriggerWorkflow() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
        DSWorkflowPayload dsWorkflowPayload = createTriggerDsWorkflowPayload();
        Optional<Long> spiWorkflowInstanceIdOpt = Optional.of(dsWorkflowId); // Optional.empty();
        IExecChainContext execChainContext = null;
        DistributeJobTriggerBuildResult triggerBuildResult = dsWorkflowPayload.triggerWorkflow(execChainContext, AssembleSvcCompsite.statusRpc);
        Assert.assertNotNull("triggerBuildResult can not be null", triggerBuildResult);
        Thread.sleep(900000);
        Mockito.verify(applicationDAO, Mockito.times(1)).selectByName(appName);
        Mockito.verify(commonDAOContext).createNewDataXTask(matchSPIExecContext(), Mockito.eq(TriggerType.CRONTAB));
        Mockito.verify(commonDAOContext).getLatestSuccessWorkflowHistory(matchSynResTarget(SynResTarget.pipeline(appName)));
        Mockito.verify(workFlowBuildHistoryDAO).updateByExampleSelective(Mockito.any(), Mockito.any());
    }


    @Test
    public void testInitializeDoplinSchedulerProcessor() {

        HttpUtils.addMockApply(0, "process-definition", "process-definition-response.json", TestDSWorkflowPayload.class);

        DSWorkflowPayload workflowPayload = createDsWorkflowPayload();
        WorkflowSPIInitializer<DSWorkflowInstance> workflowSPIInitializer = new WorkflowSPIInitializer<>(workflowPayload);


        DSWorkflowInstance wfInstance = workflowSPIInitializer.initialize();
        Assert.assertNotNull("wfInstance can not be null", wfInstance);
        Mockito.verify(applicationDAO, Mockito.times(2)).selectByName(appName);
        Mockito.verify(applicationDAO).updateByExampleSelective(matchPropExpectAppFullBuildCronTime(exceptAppPayloadContent)
                , Mockito.any(ApplicationCriteria.class));
    }


    /**
     * 测试通过TIS端执行数据同步管道配置同步到DS端
     *
     * @return
     */
    private DSWorkflowPayload createDsWorkflowPayload() {
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, appName);
        DolphinschedulerDistributedSPIDataXJobSubmit submit = new DolphinschedulerDistributedSPIDataXJobSubmit();

        commonDAOContext = Mockito.mock(ICommonDAOContext.class, Mockito.withSettings().name("commonDAOContext"));
        applicationDAO = Mockito.mock(IApplicationDAO.class, Mockito.withSettings().name("applicationDAO"));
        Mockito.when(commonDAOContext.getApplicationDAO()).thenReturn(applicationDAO);

        Application mysqlPipeline = new Application();
        mysqlPipeline.setAppId(999);
        mysqlPipeline.setProjectName(appName);

        Mockito.when(applicationDAO.selectByName(appName)).thenReturn(mysqlPipeline);

        Mockito.when(applicationDAO
                .updateByExampleSelective(matchPropExpectAppFullBuildCronTime(exceptAppPayloadContent)
                        , Mockito.any(ApplicationCriteria.class))).thenReturn(1);

        ExportTISPipelineToDolphinscheduler exportCfg = null;
        DSWorkflowPayload workflowPayload = new DSWorkflowPayload(exportCfg, dataxProcessor, commonDAOContext, submit);
        return workflowPayload;
    }

    /**
     * DS端触发数据同步通道模拟测试
     *
     * @return
     */
    private DSWorkflowPayload createTriggerDsWorkflowPayload() {
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, appName);
        DolphinschedulerDistributedSPIDataXJobSubmit submit = new DolphinschedulerDistributedSPIDataXJobSubmit();

        // commonDAOContext.createNewDataXTask(com.qlangtech.tis.plugin.datax.SPIExecContext@6b6776cb, CRONTAB):

        commonDAOContext = Mockito.mock(ICommonDAOContext.class, Mockito.withSettings().name("commonDAOContext"));
        applicationDAO = Mockito.mock(IApplicationDAO.class, Mockito.withSettings().name("applicationDAO"));

        workFlowBuildHistoryDAO = Mockito.mock(IWorkFlowBuildHistoryDAO.class
                , Mockito.withSettings().name("workFlowBuildHistoryDAO"));
        Mockito.when(workFlowBuildHistoryDAO
                        .updateByExampleSelective(Mockito.any(), Mockito.any()))
                .thenReturn(1);

        Mockito.when(commonDAOContext.getTaskBuildHistoryDAO()).thenReturn(workFlowBuildHistoryDAO);
        Mockito.when(commonDAOContext.getApplicationDAO()).thenReturn(applicationDAO);

        Mockito.when(commonDAOContext
                        .getLatestSuccessWorkflowHistory(matchSynResTarget(SynResTarget.pipeline(appName))))
                .thenReturn(null);

        Application mysqlPipeline = new Application();
        mysqlPipeline.setAppId(999);
        mysqlPipeline.setProjectName(appName);
        mysqlPipeline.setFullBuildCronTime(exceptAppPayloadContent);

        Mockito.when(applicationDAO.selectByName(appName)).thenReturn(mysqlPipeline);

        CreateNewTaskResult newTaskResult = new CreateNewTaskResult();
        newTaskResult.setTaskid(888);
        newTaskResult.setApp(mysqlPipeline);
        Mockito.when(commonDAOContext.createNewDataXTask(matchSPIExecContext(), Mockito.eq(TriggerType.CRONTAB))).thenReturn(newTaskResult);

//        Mockito.when(applicationDAO
//                .updateByExampleSelective(matchPropExpectAppFullBuildCronTime(), Mockito.any(ApplicationCriteria.class))).thenReturn(1);

        ExportTISPipelineToDolphinscheduler exportCfg = createExportTISPipelineToDolphinscheduler();
        return new DSWorkflowPayload(exportCfg, dataxProcessor, commonDAOContext, submit);

    }

    public static @NotNull ExportTISPipelineToDolphinscheduler createExportTISPipelineToDolphinscheduler() {
        ExportTISPipelineToDolphinscheduler exportCfg = new ExportTISPipelineToDolphinscheduler() {
            @Override
            public DolphinSchedulerEndpoint getDSEndpoint() {
                return TestDSWorkflowPayload.dsEndpoint;
            }
        };
        exportCfg.projectCode = String.valueOf(117442916207136l);
        return exportCfg;
    }

    private static SynResTarget matchSynResTarget(SynResTarget expect) {
        return Mockito.argThat(new SynResTargetMatcher(expect));
    }

    private static SPIExecContext matchSPIExecContext() {
        return Mockito.argThat(new TriggerWorkflowSPIExecContext());
    }

    private static Application matchPropExpectAppFullBuildCronTime(String propExpectAppFullBuildCronTime) {
        return Mockito.argThat(new ApplicationMatcher(propExpectAppFullBuildCronTime));
    }

}
