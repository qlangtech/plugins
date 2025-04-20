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

import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.test.TISEasyMock;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-31 13:48
 **/
public class TestTriggrWorkflowJobs implements TISEasyMock {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void beforeLaunch() throws Exception {
        File dataDir = folder.newFolder("dataDir");
        Config.setDataDir(dataDir.getAbsolutePath());
        IOUtils.loadResourceFromClasspath(
                TestTriggrWorkflowJobs.class, TriggrWorkflowJobs.FILE_NAME_SUBMIT_WORKFLOW_JOBS, true, (stream) -> {
                    FileUtils.copyInputStreamToFile(stream, TriggrWorkflowJobs.createSubmitWorkflowJobsFile());
                    return null;
                });
        DataxProcessor.processorGetter = (dataXName) -> {
            DefaultDataxProcessor dataxProcessor = new DefaultDataxProcessor();
            return dataxProcessor;
        };

    }

    @AfterClass
    public static void complete() throws Exception {
        DataxProcessor.processorGetter = null;
    }

    @Test
    public void testCreateTriggrWorkflowJobs() throws Exception {
        ICommonDAOContext commonDAOContext = mock("commonDAOContext", ICommonDAOContext.class);
        //Config.setTestDataDir()

        replay();

        TriggrWorkflowJobs wfJobs = TriggrWorkflowJobs.create(commonDAOContext, (className) -> {
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
        Assert.assertNotNull(wfJobs);


        Assert.assertEquals(1, wfJobs.triggrWorkflowJobs.size());

        WorkFlowBuildHistoryPayload waitCheckHistoryRecord
                = wfJobs.triggrWorkflowJobs.poll();
        Assert.assertNotNull(waitCheckHistoryRecord);
        waitCheckHistoryRecord.processExecHistoryRecord();

        verifyAll();
    }

}
