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

package com.qlangtech.tis.plugin.datax.doplinscheduler.history;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import junit.framework.TestCase;
import org.easymock.EasyMock;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-29 16:06
 **/
public class TestDSWorkFlowBuildHistoryPayload extends TestCase implements TISEasyMock {

    public void testProcessExecHistoryRecord() {
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, "mysql");
        final Integer tisTaskId = 999;
        ICommonDAOContext daoContext = mock("commonDAOContext", ICommonDAOContext.class);
        IWorkFlowBuildHistoryDAO workFlowBuildHistoryDAO = mock("workFlowBuildHistoryDAO", IWorkFlowBuildHistoryDAO.class);
        WorkFlowBuildHistory wfBuildHistory = new WorkFlowBuildHistory();

        JSONObject status = new JSONObject();
        status.put(ITISPowerJob.KEY_POWERJOB_WORKFLOW_INSTANCE_ID, 24);
        wfBuildHistory.setAsynSubTaskStatus(status.toJSONString());

        EasyMock.expect(workFlowBuildHistoryDAO.selectByPrimaryKey(tisTaskId)).andReturn(wfBuildHistory);
        EasyMock.expect(daoContext.getTaskBuildHistoryDAO()).andReturn(workFlowBuildHistoryDAO);
        //.selectByPrimaryKey(tisTaskId);


        DSWorkFlowBuildHistoryPayload wfBuildHistoryPayload = new DSWorkFlowBuildHistoryPayload(dataxProcessor, tisTaskId, daoContext);
        this.replay();
        wfBuildHistoryPayload.processExecHistoryRecord();
        this.verifyAll();
    }
}
