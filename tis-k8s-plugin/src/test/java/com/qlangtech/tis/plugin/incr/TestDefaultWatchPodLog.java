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

package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.TestK8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-12-22 12:23
 **/
public class TestDefaultWatchPodLog extends TestCase {

    public void testStartProcessPodLiveLog() throws Exception {

        Optional<String> containerId = Optional.empty();
        String podName = "datax-worker-powerjob-server-mfgk8";
        K8SDataXPowerJobServer powerJobServer = TestK8SDataXPowerJobServer.createPowerJobServer(null);
        CoreV1Api client = powerJobServer.getK8SApi();
        final K8sImage config = powerJobServer.getImage();

        DefaultWatchPodLog watchPodLog = new DefaultWatchPodLog(containerId, TargetResName.K8S_DATAX_INSTANCE_NAME, podName, client.getApiClient(), config);

        watchPodLog.addListener(new ILogListener() {
            @Override
            public void sendMsg2Client(Object biz) throws IOException {
                ExecuteState event = (ExecuteState) biz;
                System.out.println(event);
            }

            @Override
            public void read(Object event) {
                System.out.println(event);
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        });

        //   watchPodLog.startProcess();

        Thread.sleep(999000);
    }
}
