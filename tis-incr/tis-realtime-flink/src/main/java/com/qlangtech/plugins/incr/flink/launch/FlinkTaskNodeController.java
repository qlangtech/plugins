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

import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.TISFlinkCDCStart;
import com.qlangtech.plugins.incr.flink.common.FlinkCluster;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.IDeploymentDetail;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.ExtensionList;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.manage.common.incr.StreamContextConstant;
import com.qlangtech.tis.plugin.*;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugins.flink.client.FlinkClient;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.XStream2;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-20 13:39
 **/
public class FlinkTaskNodeController implements IRCController {
    private static final Logger logger = LoggerFactory.getLogger(FlinkTaskNodeController.class);
    private final TISFlinkCDCStreamFactory factory;

    public FlinkTaskNodeController(TISFlinkCDCStreamFactory factory) {
        this.factory = factory;
    }

    public static void main(String[] args) {

    }

    @Override
    public void checkUseable() {
        FlinkCluster cluster = factory.getClusterCfg();
        try {
            try (RestClusterClient restClient = cluster.createFlinkRestClusterClient(Optional.of(1000l))) {
                // restClient.getClusterId();
                CompletableFuture<Collection<JobStatusMessage>> status = restClient.listJobs();
                Collection<JobStatusMessage> jobStatus = status.get();
            }
        } catch (Exception e) {
            throw new TisException("Please check link is valid:" + cluster.getJobManagerAddress().getURL(), e);
        }
    }

    @Override
    public void deploy(TargetResName collection, ReplicasSpec incrSpec, long timestamp) throws Exception {
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(TIS.get().getPluginManager().uberClassLoader);
        try (RestClusterClient restClient = factory.getFlinkCluster()) {


            FlinkClient flinkClient = new FlinkClient();

            // File rootLibDir = new File("/Users/mozhenghua/j2ee_solution/project/plugins");
            String streamJar = StreamContextConstant.getIncrStreamJarName(collection.getName());
            // File streamJar = StreamContextConstant.getIncrStreamJarFile(collection.getName(), timestamp);
//            if (!streamJar.exists()) {
//                throw new IllegalStateException("streamJar must be exist, path:" + streamJar.getAbsolutePath());
//            }
            File streamUberJar = new File(FileUtils.getTempDirectory() + "/tmp", "uber_" + streamJar);
            logger.info("streamUberJar path:{}", streamUberJar.getAbsolutePath());
            JarSubmitFlinkRequest request = new JarSubmitFlinkRequest();
            //request.setCache(true);
            // request.setDependency(streamJar.getAbsolutePath());
            request.setParallelism(factory.parallelism);
            // request.setEntryClass("com.qlangtech.plugins.incr.flink.TISFlinkCDCStart");
            request.setEntryClass(TISFlinkCDCStart.class.getName());


//             new Manifest();
//            Map<String, Attributes> entries = manifest.getEntries();
//            Attributes attrs = new Attributes();
//            attrs.put(new Attributes.Name(collection.getName()), String.valueOf(timestamp));
//            // 传递App名称
//            entries.put(TISFlinkCDCStart.TIS_APP_NAME, attrs);
            //  JarFile jarFile = new JarFile(streamJar);
            Manifest manifest = this.createManifestCfgAttrs(collection, timestamp);

            try (JarOutputStream jaroutput = new JarOutputStream(
                    FileUtils.openOutputStream(streamUberJar, false), manifest)) {

                jaroutput.flush();
//                oJarFile.stream().forEach((f) -> {
//                    try {
//                        jaroutput.putNextEntry(new ZipEntry(collection.getName() + "/" + f.getName()));
//                        if (!f.isDirectory()) {
//                            try (InputStream content = oJarFile.getInputStream(f)) {
//                                jaroutput.write(IOUtils.toByteArray(content));
//                            }
//                        }
//                        jaroutput.closeEntry();
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                });


//                // 保证组件服务可以成功加载
//                jaroutput.putNextEntry(new ZipEntry(collection.getName() + "/" + Indexer.METAINF_ANNOTATIONS + TISExtension.class.getName()));
//                ByteArrayOutputStream bytes = null;
//                try (ObjectOutputStream output = new ObjectOutputStream(bytes = new ByteArrayOutputStream())) {
//                    output.writeObject(SerAnnotatedElementUtils.create(collection));
//                    output.writeObject(null);
//                    output.flush();
//                    jaroutput.write(bytes.toByteArray());
//                }
//                jaroutput.closeEntry();

//                JarEntry entry = new JarEntry("META-INF/");
//                entry.setTime(System.currentTimeMillis());
//                jaroutput.putNextEntry(entry);
//                jaroutput.closeEntry();
            }

//            jarFile = new JarFile(streamUberJar);
//            jarFile.stream().forEach((entry) -> {
//                System.out.println("-----" + entry.getName());
//            });

            request.setProgramArgs(collection.getName());
            request.setDependency(streamUberJar.getAbsolutePath());

            long start = System.currentTimeMillis();
            JobID jobID = flinkClient.submitJar(restClient, request);

            File incrJobFile = getIncrJobRecordFile(collection);
            FileUtils.write(incrJobFile, jobID.toHexString(), TisUTF8.get());
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }

    private Manifest createManifestCfgAttrs(TargetResName collection, long timestamp) throws Exception {

        Manifest manifest = new Manifest();
        Map<String, Attributes> entries = manifest.getEntries();
        Attributes attrs = new Attributes();
        attrs.put(new Attributes.Name(collection.getName()), String.valueOf(timestamp));
        // 传递App名称
        entries.put(TISFlinkCDCStart.TIS_APP_NAME, attrs);

        final Attributes cfgAttrs = new Attributes();
        // 传递Config变量
        Config.getInstance().visitKeyValPair((e) -> {
            if (Config.KEY_TIS_HOST.equals(e.getKey())) {
                // tishost为127.0.0.1会出错
                return;
            }
            cfgAttrs.put(new Attributes.Name(TISFlinkCDCStart.convertCfgPropertyKey(e.getKey(), true)), e.getValue());
        });
        cfgAttrs.put(new Attributes.Name(TISFlinkCDCStart.convertCfgPropertyKey(Config.KEY_TIS_HOST, true)), NetUtils.getHost());
        entries.put(Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS, cfgAttrs);

        //=====================================================================
        //  = Lists.newArrayList();
        if (!CenterResource.notFetchFromCenterRepository()) {
            throw new IllegalStateException("must not fetchFromCenterRepository");
        }

        ExtensionList<HeteroEnum> hlist = TIS.get().getExtensionList(HeteroEnum.class);
        List<IRepositoryResource> keyedPluginStores = hlist.stream()
                .filter((e) -> !e.isAppNameAware())
                .map((e) -> e.getPluginStore(null, null))
                .collect(Collectors.toList());
        ComponentMeta dataxComponentMeta = new ComponentMeta(keyedPluginStores);
        Set<XStream2.PluginMeta> globalPluginMetas = dataxComponentMeta.loadPluginMeta();
        Map<String, Long> gPluginStoreLastModify = ComponentMeta.getGlobalPluginStoreLastModifyTimestamp(dataxComponentMeta);
        // Attributes storeCfgLastModifyAttr = new Attributes();

        StringBuffer globalPluginStore = new StringBuffer();
        for (Map.Entry<String, Long> e : gPluginStoreLastModify.entrySet()) {
            globalPluginStore.append(e.getKey())
                    .append(XStream2.PluginMeta.NAME_VER_SPLIT).append(e.getValue()).append(",");
        }

        //"globalPluginStore"  "pluginMetas"  "appLastModifyTimestamp"

        PluginAndCfgsSnapshot localPluginAndCfgsSnapshot
                = PluginStore.getLocalPluginAndCfgsSnapshot(collection);

        localPluginAndCfgsSnapshot.attachPluginCfgSnapshot2Manifest(manifest);


//        final Attributes pmetas = new Attributes();
//        pmetas.put(new Attributes.Name(KeyedPluginStore.PluginMetas.KEY_GLOBAL_PLUGIN_STORE), globalPluginStore);
//        // 本次任务相关插件元信息
//        KeyedPluginStore.PluginMetas pluginMetas = KeyedPluginStore.getPluginMetas(false, collection.getName());
//        pmetas.put(new Attributes.Name(KeyedPluginStore.PluginMetas.KEY_PLUGIN_META)
//                , Sets.union(pluginMetas.metas, globalPluginMetas).stream().map((meta) -> {
//            meta.getLastModifyTimeStamp();
//            return meta.toString();
//        }).collect(Collectors.joining(",")));
//        pmetas.put(new Attributes.Name(KeyedPluginStore.PluginMetas.KEY_APP_LAST_MODIFY_TIMESTAMP)
//                , String.valueOf(pluginMetas.lastModifyTimestamp));
//
//        entries.put(Config.KEY_PLUGIN_METAS, pmetas);


        return manifest;
    }

    private File getIncrJobRecordFile(TargetResName collection) {
        DataxProcessor processor = DataxProcessor.load(null, collection.getName());
        // assertTrue("launchResult must success", launchResult.get());
        // EasyMock.verify(jarLoader);
        File dataXWorkDir = processor.getDataXWorkDir(null);
        return new File(dataXWorkDir, "incrJob.log");
    }

    @Override
    public IDeploymentDetail getRCDeployment(TargetResName collection) {
        ExtendFlinkJobDeploymentDetails rcDeployment = null;

        JobID launchJobID = getLaunchJobID(collection);
        if (launchJobID == null) {
            return null;
        }

        try {
            try (RestClusterClient restClient = this.factory.getFlinkCluster()) {
                CompletableFuture<JobStatus> jobStatus = restClient.getJobStatus(launchJobID);
                JobStatus status = jobStatus.get(5, TimeUnit.SECONDS);
//                if (status == JobStatus.CANCELED || status == JobStatus.FINISHED) {
//                    return null;
//                }
                if (status == null) {
                    return null;
                }
                CompletableFuture<JobDetailsInfo> jobDetails = restClient.getJobDetails(launchJobID);
                JobDetailsInfo jobDetailsInfo = jobDetails.get(5, TimeUnit.SECONDS);
                rcDeployment = new ExtendFlinkJobDeploymentDetails(factory.getClusterCfg(), jobDetailsInfo);
                return rcDeployment;
            }
        } catch (TimeoutException e) {
            FlinkCluster clusterCfg = this.factory.getClusterCfg();
            throw new TisException("flinkClusterId:" + clusterCfg.getClusterId()
                    + ",Address:" + clusterCfg.getJobManagerAddress().getURL() + "连接超时，请检查相应配置是否正确", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (StringUtils.indexOf(cause.getMessage(), "NotFoundException") > -1) {
                return null;
            }
//            if (cause instanceof RestClientException) {
//                //cause.getStackTrace()
//                if (ExceptionUtils.indexOfType(cause, FlinkJobNotFoundException.class) > -1) {
//                    logger.warn("flink JobId:" + launchJobID.toHexString() + " relevant job instant is not found on ServerSize");
//                    return null;
//                }
//
//            }
            throw new RuntimeException(e);
//            if (cause instanceof ) {
//
//            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JobID getLaunchJobID(TargetResName collection) {
        try {
            File incrJobFile = getIncrJobRecordFile(collection);
            if (!incrJobFile.exists()) {
                return null;
            }
            return JobID.fromHexString(FileUtils.readFileToString(incrJobFile, TisUTF8.get()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void writeJarEntry(JarOutputStream jarOutput, String entryPath, File jarFile) {

        try {
            byte[] data = FileUtils.readFileToByteArray(jarFile);

            // ZipPath zipPath = fileObj.getZipPath();
            JarEntry entry = new JarEntry(entryPath);
            entry.setTime(System.currentTimeMillis());
            //byte[] data = fileObj.getOutputStream().toByteArray();
            entry.setSize(data.length);
            CRC32 crc = new CRC32();
            crc.update(data);
            entry.setCrc(crc.getValue());
            jarOutput.putNextEntry(entry);
            jarOutput.write(data);
            jarOutput.closeEntry();
        } catch (IOException e) {
            throw new RuntimeException(jarFile.getAbsolutePath(), e);
        }
    }

//    private static SubModule $(String dir, String module) {
//        return new SubModule(dir, module);
//    }
//
//    private static class SubModule {
//        private final String dir;
//        private final String module;
//
//        public SubModule(String dir, String module) {
//            this.dir = dir;
//            this.module = module;
//        }
//
//        public String getFullModuleName() {
//            return dir + "/" + module;
//        }
//
//        @Override
//        public String toString() {
//            return "SubModule{" +
//                    "dir='" + dir + '\'' +
//                    ", module='" + module + '\'' +
//                    '}';
//        }
//    }

    @Override
    public void removeInstance(TargetResName collection) throws Exception {
        try {
            JobID launchJobID = getLaunchJobID(collection);
            if (launchJobID == null) {
                throw new IllegalStateException("have not found any launhed job,app:" + collection.getName());
            }
            try (RestClusterClient restClient = this.factory.getFlinkCluster()) {
                CompletableFuture<JobStatus> jobStatus = restClient.getJobStatus(launchJobID);
                JobStatus s = jobStatus.get(5, TimeUnit.SECONDS);
                if (s != null && !s.isTerminalState()) {
                    //job 任务没有终止，立即停止
                    CompletableFuture<Acknowledge> result = restClient.cancel(launchJobID);
                    result.get(5, TimeUnit.SECONDS);
                }
                File incrJobFile = getIncrJobRecordFile(collection);
                FileUtils.forceDelete(incrJobFile);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void relaunch(TargetResName collection, String... targetPod) {

    }


    @Override
    public WatchPodLog listPodAndWatchLog(TargetResName collection, String podName, ILogListener listener) {
        return null;
    }

}
