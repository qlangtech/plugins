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

import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.test.TISEasyMock;
import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.junit.Assert;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-04-13 16:03
 **/
public class TestDefaultDataxSplitTabSyncConsumer extends TestCase implements TISEasyMock {

    public void testGetExtraJavaSystemPrams() {

        IExecChainContext execContext = mock("execContext", IExecChainContext.class);
        LocalDataXJobSubmit jobSubmit = mock("jobSubmit", LocalDataXJobSubmit.class);

        ReplicasSpec spec = new ReplicasSpec();
        spec.setMemoryLimit(Specification.parse("1.5G"));
        spec.setMemoryRequest(Specification.parse("1G"));

        EasyMock.expect(execContext.getJavaMemSpec()).andReturn(spec.toJavaMemorySpec(Optional.empty()));

        String[] sysParams = new String[]{"-DtestParam=xxxx"};
        EasyMock.expect(jobSubmit.getExtraJavaSystemPrams()).andReturn(sysParams);


        DefaultDataxSplitTabSyncConsumer syncConsumer = new DefaultDataxSplitTabSyncConsumer(execContext, jobSubmit);

        this.replay();
        String[] extraJavaSystemPrams = syncConsumer.getExtraJavaSystemPrams();
        Assert.assertEquals(3, extraJavaSystemPrams.length);
        Assert.assertEquals("-Xms819m -Xmx1228m -DtestParam=xxxx",String.join(" ",extraJavaSystemPrams));
        this.verifyAll();
    }
}
