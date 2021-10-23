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

package com.qlangtech.plugins.incr.flink.launch;

import com.google.common.collect.Lists;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.RcDeployment;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.manage.common.incr.StreamContextConstant;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugins.flink.client.FlinkClient;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import com.qlangtech.tis.trigger.jst.ILogListener;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.messages.Acknowledge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-20 13:39
 **/
public class FlinkTaskNodeController implements IRCController //, FlinkSourceHandleSetter
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkTaskNodeController.class);
    private final TISFlinkCDCStreamFactory factory;

    public FlinkTaskNodeController(TISFlinkCDCStreamFactory factory) {
        this.factory = factory;
    }


    @Override
    public void deploy(TargetResName collection, ReplicasSpec incrSpec, long timestamp) throws Exception {

        try (RestClusterClient restClient = factory.getFlinkCluster()) {

            FlinkClient flinkClient = new FlinkClient();

            File rootLibDir = new File("/Users/mozhenghua/j2ee_solution/project/plugins");

            File streamJar = StreamContextConstant.getIncrStreamJarFile(collection.getName(), timestamp);
            File streamUberJar = new File(FileUtils.getTempDirectory() + "/tmp", "uber_" + streamJar.getName());
            if (!streamJar.exists()) {
                throw new IllegalStateException("streamJar must be exist, path:" + streamJar.getAbsolutePath());
            }

            JarSubmitFlinkRequest request = new JarSubmitFlinkRequest();
            //request.setCache(true);
            request.setDependency(streamJar.getAbsolutePath());
            request.setParallelism(factory.parallelism);
            request.setEntryClass("com.qlangtech.plugins.incr.flink.TISFlinkCDCStart");
            // List<URL> classPaths = Lists.newArrayList();
            // classPaths.add((new File("/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-cdc-plugin/target/tis-flink-cdc-plugin/WEB-INF/lib")).toURL());

            // SubModule[] subModules = new SubModule[]{
//                $("tis-datax", "tis-datax-elasticsearch-plugin") //
//                , $("tis-incr", "tis-elasticsearch7-sink-plugin") //
//                , $("tis-incr", "tis-realtime-flink") //
//                , $("tis-incr", "tis-flink-cdc-plugin") //
            //        , $("tis-incr", "tis-realtime-flink-launch")
            //};
            File subDir = null;
            // Set<String> addedFiles = Sets.newHashSet();
            List<File> classpaths = Lists.newArrayList();
//        for (SubModule sub : subModules) {
//            subDir = new File(rootLibDir, sub.getFullModuleName() + "/target/" + sub.module + "/WEB-INF/lib");
//            if (subDir.exists()) {
//                for (File f : subDir.listFiles()) {
//                    if (addedFiles.add(f.getName())) {
//                        classpaths.add(f);
//                    }
//                }
//                continue;
//            } else {
//                subDir = new File(rootLibDir, sub.getFullModuleName() + "/target/dependency");
//                if (subDir.exists()) {
//                    for (File f : subDir.listFiles()) {
//                        if (addedFiles.add(f.getName())) {
//                            classpaths.add(f);
//                        }
//                    }
//                    continue;
//                }
//            }
//            throw new IllegalStateException("subModule is illegal:" + sub);
//        }

//            classpaths.forEach((f) -> {
//                System.out.println(f.getAbsolutePath());
//            });
//            System.out.println("==============================");

            //     FileUtils.copyFile(streamJar, streamUberJar);
            try (JarOutputStream jaroutput = new JarOutputStream(FileUtils.openOutputStream(streamUberJar, true))) {


//            try (JarInputStream jarFile = new JarInputStream(FileUtils.openInputStream(streamJar))) {
//                JarEntry entry = null;
//                while ((entry = jarFile.getNextJarEntry()) != null) {
//                    jaroutput.putNextEntry(entry);
//                    if(!entry.isDirectory()){
//                        jarFile.read()
//                        jaroutput.write(entry.getSize());
//                    }
//                    jaroutput.closeEntry();
//                }
//            }
                JarFile jarFile = new JarFile(streamJar);
                jarFile.stream().forEach((f) -> {
                    try {
                        jaroutput.putNextEntry(f);
                        if (!f.isDirectory()) {
                            try (InputStream content = jarFile.getInputStream(f)) {
                                jaroutput.write(IOUtils.toByteArray(content));
                            }
                        }
                        jaroutput.closeEntry();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                JarEntry entry = new JarEntry("lib/");
                entry.setTime(System.currentTimeMillis());
                jaroutput.putNextEntry(entry);
                jaroutput.closeEntry();

                classpaths.forEach(cp -> {
                    writeJarEntry(jaroutput, "lib/" + cp.getName(), cp);
                    try {
                        FileUtils.copyFile(cp, new File(streamUberJar.getParentFile(), "lib/" + cp.getName()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    //  try {
//                    //JarEntry entry = new JarEntry(p + "/");
//                    entry.setTime(System.currentTimeMillis());
//                    if (savedEntryPaths.add(entry.getName())) {
//                        jaroutput.putNextEntry(entry);
//                        jaroutput.closeEntry();
//                    }
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
                });
            }

            JarFile jarFile = new JarFile(streamUberJar);
            jarFile.stream().forEach((entry) -> {
                System.out.println("-----" + entry.getName());
            });

            request.setProgramArgs(collection.getName());
            request.setDependency(streamUberJar.getAbsolutePath());

            long start = System.currentTimeMillis();
            JobID jobID = flinkClient.submitJar(restClient, request);

            File incrJobFile = getIncrJobRecordFile(collection);
            FileUtils.write(incrJobFile, jobID.toHexString(), TisUTF8.get());
        }
    }

    private File getIncrJobRecordFile(TargetResName collection) {
        DataxProcessor processor = DataxProcessor.load(null, collection.getName());
        // assertTrue("launchResult must success", launchResult.get());
        // EasyMock.verify(jarLoader);
        File dataXWorkDir = processor.getDataXWorkDir(null);
        return new File(dataXWorkDir, "incrJob.log");
    }

    @Override
    public RcDeployment getRCDeployment(TargetResName collection) {
        RcDeployment rcDeployment = null;

        JobID launchJobID = getLaunchJobID(collection);
        if (launchJobID == null) return null;

        try {
            try (RestClusterClient restClient = this.factory.getFlinkCluster()) {
                CompletableFuture<JobStatus> jobStatus = restClient.getJobStatus(launchJobID);
                JobStatus status = jobStatus.get(5, TimeUnit.SECONDS);
                if (status == JobStatus.CANCELED || status == JobStatus.FINISHED) {
                    return null;
                }
                rcDeployment = new RcDeployment();
                return rcDeployment;
            }
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
                CompletableFuture<Acknowledge> result = restClient.cancel(launchJobID);
                result.get(5, TimeUnit.SECONDS);
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
