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

package com.qlangtech.plugins.incr.flink.cluster;

import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.trigger.jst.ILogListener;

import java.util.List;

/**
 * 作为kubernetes-application 的配置模版存储
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-13 08:45
 **/
public class KubernetesApplicationClusterConfig extends BasicFlinkK8SClusterCfg implements IdentityName {

    //    @FormField(ordinal = 1, validate = {Validator.require})
//    public KubernetesApplicationClusterSaveAsCfg saveAsCfg;
    @FormField(ordinal = 1, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String configName;

    @Override
    public String identityValue() {
        return this.configName;
    }

    @Override
    public void relaunch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relaunch(String podName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RcDeployment> getRCDeployments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void launchService(SSERunnable launchProcess) {
        throw new UnsupportedOperationException();
    }

    @TISExtension()
    public static class DescriptorImpl extends BasicFlinkCfgDescriptor {

        public DescriptorImpl() {
            super();
        }

        @Override
        public K8SWorkerCptType getWorkerCptType() {
            return K8SWorkerCptType.FlinkKubernetesApplicationCfg;
        }
    }
}
