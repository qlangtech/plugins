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

package com.qlangtech.plugins.incr.flink.launch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-04-15 18:30
 **/
public class TestFlinkIncrJobStatus {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCreateNewJob() throws Exception {

        File incrJobFile = folder.newFile();

        FlinkIncrJobStatus incrJobStatus = new FlinkIncrJobStatus(incrJobFile);

        JobID jobid = JobID.fromHexString("ac99018a3dfd65fde538133d91ca58d7");
        incrJobStatus.createNewJob(jobid);
        Assert.assertEquals(FlinkIncrJobStatus.State.RUNNING, incrJobStatus.getState());

        String savePointDir = "file:///opt/data/savepoint/savepoint_20220415182209246";
        incrJobStatus.stop(savePointDir);
        Assert.assertEquals(FlinkIncrJobStatus.State.STOPED, incrJobStatus.getState());

        Assert.assertTrue(CollectionUtils.isEqualCollection(Collections.singletonList(savePointDir), incrJobStatus.getSavepointPaths()));

        // 重新加载
        incrJobStatus = new FlinkIncrJobStatus(incrJobFile);
        Assert.assertEquals(FlinkIncrJobStatus.State.STOPED, incrJobStatus.getState());
        Assert.assertTrue(CollectionUtils.isEqualCollection(Collections.singletonList(savePointDir), incrJobStatus.getSavepointPaths()));
        Assert.assertEquals(jobid, incrJobStatus.getLaunchJobID());

        JobID relaunchJobid = JobID.fromHexString("ac99018a3dfd65fde538133d91ca58d6");
        incrJobStatus.relaunch(relaunchJobid);

        Assert.assertEquals(relaunchJobid, incrJobStatus.getLaunchJobID());
        Assert.assertEquals(FlinkIncrJobStatus.State.RUNNING, incrJobStatus.getState());
        Assert.assertTrue(CollectionUtils.isEqualCollection(Collections.singletonList(savePointDir), incrJobStatus.getSavepointPaths()));
        // 重新加载
        incrJobStatus = new FlinkIncrJobStatus(incrJobFile);
        Assert.assertEquals(relaunchJobid, incrJobStatus.getLaunchJobID());
        Assert.assertEquals(FlinkIncrJobStatus.State.RUNNING, incrJobStatus.getState());
        Assert.assertTrue(CollectionUtils.isEqualCollection(Collections.singletonList(savePointDir), incrJobStatus.getSavepointPaths()));

        incrJobStatus.cancel();
        Assert.assertNull(incrJobStatus.getLaunchJobID());
        Assert.assertEquals(FlinkIncrJobStatus.State.NONE, incrJobStatus.getState());

        // 重新加载
        incrJobStatus = new FlinkIncrJobStatus(incrJobFile);
        Assert.assertNull(incrJobStatus.getLaunchJobID());

        Assert.assertTrue(CollectionUtils.isEmpty(incrJobStatus.getSavepointPaths()));
        Assert.assertEquals(FlinkIncrJobStatus.State.NONE, incrJobStatus.getState());
    }
}
