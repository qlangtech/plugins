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

package com.qlangtech.plugins.incr.flink.launch.clustertype;

import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.config.flink.IFlinkClusterConfig;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

import java.io.File;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:36
 * @see KubernetesApplication
 * @see KubernetesSession
 * @see Standalone
 **/
public abstract class ClusterType implements Describable<ClusterType>, IFlinkClusterConfig {

    public abstract void checkUseable() throws TisException;


    public abstract ClusterClient createRestClusterClient();

    /**
     * 部署flinkJob
     *
     * @param collection
     * @param streamUberJar
     * @param requestSetter
     * @param afterSuccess
     */
    public abstract void deploy(TISFlinkCDCStreamFactory factory, TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSuccess) throws Exception;

    public abstract FlinkClusterType getClusterType();
}
