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

import com.qlangtech.tis.assemble.TriggerType;
import com.qlangtech.tis.coredefine.module.action.PowerjobTriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.biz.dal.pojo.ApplicationCriteria;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.CreateNewTaskResult;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.datax.SPIExecContext;
import com.qlangtech.tis.plugin.datax.WorkflowSPIInitializer;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.ExportTISPipelineToDolphinscheduler;
import com.qlangtech.tis.realtime.yarn.rpc.SynResTarget;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.junit.Assert;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 22:29
 **/
public class TestDSWorkflowPayload extends TestCase implements TISEasyMock {

    private static final String appName = "mysql";

    private static final long dsWorkflowId = 117631790563520l;

    private static final String exceptAppPayloadContent = "{\n" +
            "\t\"workflow_id\":" + dsWorkflowId + ",\n" +
            "\t\"execRange\":[\n" +
            "\t\t\"dump\",\n" +
            "\t\t\"dump\"\n" +
            "\t]\n" +
            "}";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearMocks();
    }

    public void testTriggerWorkflow() {
        CenterResource.setNotFetchFromCenterRepository();
        DSWorkflowPayload dsWorkflowPayload = createTriggerDsWorkflowPayload();
        Optional<Long> spiWorkflowInstanceIdOpt = Optional.of(dsWorkflowId); // Optional.empty();
        PowerjobTriggerBuildResult triggerBuildResult = dsWorkflowPayload.triggerWorkflow(spiWorkflowInstanceIdOpt, AssembleSvcCompsite.statusRpc);
        Assert.assertNotNull("triggerBuildResult can not be null", triggerBuildResult);

        verifyAll();
    }

    public void testInitializeDoplinSchedulerProcessor() {

        HttpUtils.addMockApply(0, "process-definition", "process-definition-response.json", TestDSWorkflowPayload.class);

        DSWorkflowPayload workflowPayload = createDsWorkflowPayload();
        WorkflowSPIInitializer<DSWorkflowInstance> workflowSPIInitializer = new WorkflowSPIInitializer<>(workflowPayload);


        DSWorkflowInstance wfInstance = workflowSPIInitializer.initialize();
        Assert.assertNotNull("wfInstance can not be null", wfInstance);
        verifyAll();
    }


    /**
     * 测试通过TIS端执行数据同步管道配置同步到DS端
     *
     * @return
     */
    private DSWorkflowPayload createDsWorkflowPayload() {
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, appName);
        DolphinschedulerDistributedSPIDataXJobSubmit submit = new DolphinschedulerDistributedSPIDataXJobSubmit();

        ICommonDAOContext commonDAOContext = mock("commonDAOContext", ICommonDAOContext.class);
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);
        EasyMock.expect(commonDAOContext.getApplicationDAO()).andReturn(applicationDAO);

        Application mysqlPipeline = new Application();
        mysqlPipeline.setAppId(999);
        mysqlPipeline.setProjectName(appName);

        EasyMock.expect(applicationDAO.selectByName(appName)).andReturn(mysqlPipeline).times(2);

        EasyMock.expect(applicationDAO
                .updateByExampleSelective(matchPropExpectAppFullBuildCronTime(exceptAppPayloadContent)
                        , EasyMock.anyObject(ApplicationCriteria.class))).andReturn(1);

        replay();
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

        ICommonDAOContext commonDAOContext = mock("commonDAOContext", ICommonDAOContext.class);
        IApplicationDAO applicationDAO = mock("applicationDAO", IApplicationDAO.class);

        IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO
                = mock("workFlowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);
        EasyMock.expect(workFlowBuildHistoryDAO
                        .updateByExampleSelective(EasyMock.anyObject(), EasyMock.anyObject()))
                .andReturn(1);

        EasyMock.expect(commonDAOContext.getTaskBuildHistoryDAO()).andReturn(workFlowBuildHistoryDAO);
        EasyMock.expect(commonDAOContext.getApplicationDAO()).andReturn(applicationDAO);

        EasyMock.expect(commonDAOContext
                        .getLatestSuccessWorkflowHistory(matchSynResTarget(SynResTarget.pipeline(appName))))
                .andReturn(null);

        Application mysqlPipeline = new Application();
        mysqlPipeline.setAppId(999);
        mysqlPipeline.setProjectName(appName);
        mysqlPipeline.setFullBuildCronTime(exceptAppPayloadContent);

        EasyMock.expect(applicationDAO.selectByName(appName)).andReturn(mysqlPipeline).times(1);

        CreateNewTaskResult newTaskResult = new CreateNewTaskResult();
        newTaskResult.setTaskid(888);
        newTaskResult.setApp(mysqlPipeline);
        EasyMock.expect(commonDAOContext.createNewDataXTask(matchSPIExecContext(), EasyMock.eq(TriggerType.CRONTAB))).andReturn(newTaskResult);

//        EasyMock.expect(applicationDAO
//                .updateByExampleSelective(matchPropExpectAppFullBuildCronTime(), EasyMock.anyObject(ApplicationCriteria.class))).andReturn(1);

        replay();
        ExportTISPipelineToDolphinscheduler exportCfg = null;
        return new DSWorkflowPayload(exportCfg, dataxProcessor, commonDAOContext, submit);

    }

    private static SynResTarget matchSynResTarget(SynResTarget expect) {
        EasyMock.reportMatcher(new SynResTargetMatcher(expect));
        return null;
    }

    private static SPIExecContext matchSPIExecContext() {
        EasyMock.reportMatcher(new TriggerWorkflowSPIExecContext());
        return null;
    }

    private static Application matchPropExpectAppFullBuildCronTime(String propExpectAppFullBuildCronTime) {
        EasyMock.reportMatcher(new ApplicationMatcher(propExpectAppFullBuildCronTime));
        return null;
    }

}
