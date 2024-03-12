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

package com.qlangtech.tis.plugin.k8s;

import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.k8s.K8SUtils.WaitReplicaControllerLaunch;
import com.qlangtech.tis.plugin.k8s.K8sImage.ImageCategory;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-03 12:02
 **/
public class TestK8SUtils extends TestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        SSERunnable.setLocalThread(SSERunnable.createMock());
    }

    public void testTargetResName() {
        K8SRCResName rcResName = K8SUtils.targetResName(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_WORKER);
        String podName = "powerjob-worker-ck8q5";

        Assert.assertTrue(rcResName.isPodMatch(podName));

        Optional<String> podRes = rcResName.findPodResName("Created pod: powerjob-worker-ck8q5");
        Assert.assertTrue(podRes.isPresent());
        Assert.assertEquals(podName, podRes.get());
    }

    public void testWaitReplicaControllerLaunch() throws Exception {

        DefaultK8SImage powerjobServerImage = (DefaultK8SImage) ImageCategory.DEFAULT_POWERJOB_DESC_NAME.getPluginStore().find("aliyun");
        TargetResName targetResName = K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER;
        ReplicasSpec powerjobServerSpec = new ReplicasSpec();
        powerjobServerSpec.setReplicaCount(1);
        CoreV1Api coreApi = new CoreV1Api(powerjobServerImage.createApiClient());
        TargetResName resName = new TargetResName("powerjob-server");
//         appLabelSelector
//                = NamespacedEventCallCriteria.createAppTimestampLabelSelector(resName, 1709959541333l);

        NamespacedEventCallCriteria appLabelSelector = NamespacedEventCallCriteria.createResVersion("ownerUid", "15431267");

        System.out.println(appLabelSelector);

        WaitReplicaControllerLaunch waitReplicaControllerLaunch
                = K8SUtils.waitReplicaControllerLaunch(powerjobServerImage, targetResName, powerjobServerSpec, coreApi, appLabelSelector);
        Assert.assertNotNull("waitReplicaControllerLaunch", waitReplicaControllerLaunch);
    }
}
