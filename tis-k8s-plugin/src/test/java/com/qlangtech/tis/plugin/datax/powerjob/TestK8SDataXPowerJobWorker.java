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

package com.qlangtech.tis.plugin.datax.powerjob;


import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import junit.framework.TestCase;
import org.junit.Assert;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER_SERVICE;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-12-21 10:09
 **/
public class TestK8SDataXPowerJobWorker extends TestCase {
    public void testGetK8SDataXPowerJobWorker() {
        K8SDataXPowerJobWorker pjWorker = K8SUtils.getK8SDataXPowerJobWorker();
        Assert.assertNotNull(pjWorker);
    }

    public void testGetPowerJobServerHostReplacement() {
        DefaultK8SImage powerJobImage = new DefaultK8SImage();
        powerJobImage.namespace = "default";
        final String powerJobServerHostReplacement = K8S_DATAX_POWERJOB_SERVER_SERVICE.getHostPortReplacement(powerJobImage);
        Assert.assertEquals("powerjob-server-service.default:$(POWERJOB_SERVER_SERVICE_SERVICE_PORT)", powerJobServerHostReplacement);
    }
}
