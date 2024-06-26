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

package com.qlangtech.tis.plugin.datax.powerjob.impl.serverport;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import junit.framework.TestCase;
import org.apache.commons.lang3.tuple.Pair;
import org.easymock.EasyMock;

import java.util.Optional;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-26 09:25
 **/
public class TestNodePort extends TestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        SSERunnable.setLocalThread(SSERunnable.createMock());
    }

    public void testExportPort() throws Exception {
        NodePort nodePort = new NodePort();
        nodePort.serverPort = 7700;
        nodePort.host = "192.168.203.175";
        nodePort.nodePort = 31000;
        // K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE

        CoreV1Api api = EasyMock.mock("api", CoreV1Api.class);

//        String nameSpace, CoreV1Api api, String targetPortName
//                , Pair<ServiceResName, TargetResName> serviceResAndOwner, Optional<V1OwnerReference> ownerRef

        TargetResName resOwner = new TargetResName("test");

        EasyMock.replay(api);

        V1OwnerReference ownerReference = K8SUtils.createOwnerReference("testOwnerId", "testOwnerName");

        nodePort.exportPort("tis", api, "targetPortName"
                , Pair.of(K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE, resOwner), Optional.of(ownerReference));

        EasyMock.verify(api);
    }
}
