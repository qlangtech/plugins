/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.plugins.incr.flink.cluster;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Maps;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.jst.ILogListener;
import org.apache.commons.io.IOUtils;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.*;
import org.apache.flink.kubernetes.cli.KubernetesSessionCli;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;

/**
 * Flink Cluster 管理
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-04 14:30
 **/
public class FlinkK8SClusterManager extends DataXJobWorker {

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String clusterId;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer jmMemory;

    @FormField(ordinal = 4, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public Integer tmMemory;


    @Override
    public void launchService() {
        try {

            K8sImage k8SImageCfg = this.getK8SImage();

            final Configuration configuration = GlobalConfiguration.loadConfiguration();
            //1600
            configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(jmMemory));
            //1728
            configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.ofMebiBytes(tmMemory));
            // "registry.cn-hangzhou.aliyuncs.com/tis/flink-3.1.0:latest"
            configuration.set(KubernetesConfigOptions.CONTAINER_IMAGE, k8SImageCfg.getImagePath());
            configuration.set(KubernetesConfigOptions.CLUSTER_ID, clusterId);
            configuration.set(KubernetesConfigOptions.NAMESPACE, k8SImageCfg.getNamespace());


            FlinkConfMountDecorator.configMapData = getCreateAccompanyConfigMapResource();

            // configuration.set(KubernetesConfigOptions.CONTAINER_IMAGE, "apache/flink:1.13.2-scala_2.11");
            //  configuration.set(KubernetesConfigOptions.NAMESPACE, "registry.cn-hangzhou.aliyuncs.com/tis/flink-3.1.0:latest");

            final String configDir = CliFrontend.getConfigurationDirectoryFromEnv();

            IK8sContext kubeConfig = k8SImageCfg.getK8SCfg();// ParamsConfig.getItem("minikube", IK8sContext.class);
            FlinkKubeClientFactory.kubeConfig = io.fabric8.kubernetes.client.Config.fromKubeconfig(kubeConfig.getKubeConfigContent());

            final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, configDir);

            cli.run(new String[]{});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void relaunch() {

        //args = new String[]{"--" + JobManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=1600m"};
        // CenterResource.setNotFetchFromCenterRepository();

    }

    @Override
    public void relaunch(String podName) {

    }

    @Override
    public RcDeployment getRCDeployment() {
        return null;
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        return null;
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        return null;
    }

    @Override
    public String getZookeeperAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getZkQueuePath() {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean inService() {
        return false;
    }

    @Override
    public void remove() {

    }


    private Map<String, String> getCreateAccompanyConfigMapResource() throws IOException {
        Map<String, String> configMap = Maps.newHashMap();
        addResFromCP(configMap, CONFIG_FILE_LOGBACK_NAME);
        addResFromCP(configMap, CONFIG_FILE_LOG4J_NAME);
        return configMap;
    }

    private void addResFromCP(Map<String, String> configMap, String configFileLogbackName) throws IOException {
        try (InputStream input = FlinkK8SClusterManager.class.getResourceAsStream(configFileLogbackName)) {
            configMap.put(configFileLogbackName, IOUtils.toString(input, TisUTF8.get()));
        }
    }

    @TISExtension()
    public static class DescriptorImpl extends BasicDescriptor {

        private static final MemorySize MEMORY_8G = MemorySize.ofMebiBytes(8 * 1024);

        public DescriptorImpl() {
            super();
        }

        public boolean validateJmMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            MemorySize zero = MemorySize.ofMebiBytes(0);
            MemorySize memory = MemorySize.ofMebiBytes(Integer.parseInt(value));
            if (MEMORY_8G.compareTo((memory)) > 0) {
                msgHandler.addFieldError(context, fieldName, "内存不能大于:" + MEMORY_8G.toHumanReadableString());
                return false;
            }
            if (zero.compareTo(memory) <= 0) {
                msgHandler.addFieldError(context, fieldName, "内存不能小于:" + zero.toHumanReadableString());
                return false;
            }
            return true;
        }

        public boolean validateTmMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateJmMemory(msgHandler, context, fieldName, value);
        }


        @Override
        public final String getDisplayName() {
            return "Flink-Cluster";
        }
    }
}
