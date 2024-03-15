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

package com.qlangtech.plugins.incr.flink.common;

import com.qlangtech.plugins.incr.flink.cluster.BasicFlinkK8SClusterCfg;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.extension.TISExtension;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-04 08:49
 **/
public class FlinkK8SImage extends DefaultK8SImage {

    public AppsV1Api createAppsV1Api() {
        return new AppsV1Api(this.createApiClient());
    }

    public CoreV1Api createCoreV1Api() {
        return new CoreV1Api(this.createApiClient());
    }

    @TISExtension()
    public static class FlinkDescriptorImpl extends DescriptorImpl {
        @Override
        protected ImageCategory getImageCategory() {
            return BasicFlinkK8SClusterCfg.k8sImage();
        }
    }
}
