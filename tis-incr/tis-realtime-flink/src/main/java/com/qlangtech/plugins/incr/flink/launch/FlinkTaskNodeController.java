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
import com.google.common.collect.Sets;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.RcDeployment;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugins.flink.client.FlinkClient;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.FlinkSourceHandleSetter;
import com.qlangtech.tis.trigger.jst.ILogListener;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-20 13:39
 **/
public class FlinkTaskNodeController implements IRCController, FlinkSourceHandleSetter {

    private final TISFlinkCDCStreamFactory factory;

    public FlinkTaskNodeController(TISFlinkCDCStreamFactory factory) {
        this.factory = factory;
    }

    @Override
    public void deploy(TargetResName collection, ReplicasSpec incrSpec, long timestamp) throws Exception {

        String[] address = StringUtils.split(factory.jobManagerAddress, ":");
        if (address.length != 2) {
            throw new IllegalArgumentException("illegal jobManagerAddress:" + factory.jobManagerAddress);
        }
        Configuration configuration = new Configuration();
        int port = Integer.parseInt(address[1]);
        configuration.setString(JobManagerOptions.ADDRESS, address[0]);
        configuration.setInteger(JobManagerOptions.PORT, port);
        configuration.setInteger(RestOptions.PORT, port);


        //int port = 32440;
//        int port = 8081;
//        configuration.setString(JobManagerOptions.ADDRESS, "192.168.28.201");
//        configuration.setInteger(JobManagerOptions.PORT, port);
//        configuration.setInteger(RestOptions.PORT, port);
        RestClusterClient restClient = new RestClusterClient<>(configuration, factory.clusterId);
//        restClient.setPrintStatusDuringExecution(true);
//        restClient.setDetached(true);

        FlinkClient flinkClient = new FlinkClient();
        // JarLoader jarLoader = EasyMock.createMock("jarLoader", JarLoader.class);

//        File streamJar = new File("/tmp/TopSpeedWindowing.jar");

        File rootLibDir = new File("/Users/mozhenghua/j2ee_solution/project/plugins");

        File streamJar = new File(rootLibDir, "tis-incr/tis-realtime-flink-launch/target/tis-realtime-flink-launch.jar");
        File streamUberJar = new File(FileUtils.getTempDirectory() + "/tmp", "uber_" + streamJar.getName());
        // assertTrue(, streamJar.exists());
        if (!streamJar.exists()) {
            throw new IllegalStateException("streamJar must be exist, path:" + streamJar.getAbsolutePath());
        }

        //EasyMock.expect(jarLoader.downLoad(EasyMock.anyString(), EasyMock.eq(true))).andReturn(streamUberJar);
        //flinkClient.setJarLoader(jarLoader);

        JarSubmitFlinkRequest request = new JarSubmitFlinkRequest();
        //request.setCache(true);
        request.setDependency(streamJar.getAbsolutePath());
        request.setParallelism(factory.parallelism);
        request.setEntryClass("com.qlangtech.plugins.incr.flink.TISFlinkCDCStart");
        // List<URL> classPaths = Lists.newArrayList();
        // classPaths.add((new File("/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-cdc-plugin/target/tis-flink-cdc-plugin/WEB-INF/lib")).toURL());

        SubModule[] subModules = new SubModule[]{
//                $("tis-datax", "tis-datax-elasticsearch-plugin") //
//                , $("tis-incr", "tis-elasticsearch7-sink-plugin") //
//                , $("tis-incr", "tis-realtime-flink") //
//                , $("tis-incr", "tis-flink-cdc-plugin") //
                //        , $("tis-incr", "tis-realtime-flink-launch")
        };
        File subDir = null;
        Set<String> addedFiles = Sets.newHashSet();
        List<File> classpaths = Lists.newArrayList();
        for (SubModule sub : subModules) {
            subDir = new File(rootLibDir, sub.getFullModuleName() + "/target/" + sub.module + "/WEB-INF/lib");
            if (subDir.exists()) {
                for (File f : subDir.listFiles()) {
                    if (addedFiles.add(f.getName())) {
                        classpaths.add(f);
                    }
                }
                continue;
            } else {
                subDir = new File(rootLibDir, sub.getFullModuleName() + "/target/dependency");
                if (subDir.exists()) {
                    for (File f : subDir.listFiles()) {
                        if (addedFiles.add(f.getName())) {
                            classpaths.add(f);
                        }
                    }
                    continue;
                }
            }
            throw new IllegalStateException("subModule is illegal:" + sub);
        }

        classpaths.forEach((f) -> {
            System.out.println(f.getAbsolutePath());
        });
        System.out.println("==============================");

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


        request.setDependency(streamUberJar.getAbsolutePath());

//        request.setUserClassPaths(classpaths.stream().map((f) -> {
//            try {
//                return f.toURL();
//            } catch (MalformedURLException e) {
//                throw new RuntimeException(e);
//            }
//        }).collect(Collectors.toList()));

        //   EasyMock.replay(jarLoader);
        long start = System.currentTimeMillis();
        //  System.out.println("start launch");
        AtomicBoolean launchResult = new AtomicBoolean();
        flinkClient.submitJar(restClient, request, (r) -> {
            launchResult.set(r.isSuccess());
        });

        // assertTrue("launchResult must success", launchResult.get());

        // EasyMock.verify(jarLoader);
        if (!launchResult.get()) {
            throw new IllegalStateException("deploy flink app falid:" + collection.getName());
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

    private static SubModule $(String dir, String module) {
        return new SubModule(dir, module);
    }

    private static class SubModule {
        private final String dir;
        private final String module;

        public SubModule(String dir, String module) {
            this.dir = dir;
            this.module = module;
        }

        public String getFullModuleName() {
            return dir + "/" + module;
        }

        @Override
        public String toString() {
            return "SubModule{" +
                    "dir='" + dir + '\'' +
                    ", module='" + module + '\'' +
                    '}';
        }
    }

    @Override
    public void removeInstance(TargetResName collection) throws Exception {

    }

    @Override
    public void relaunch(TargetResName collection, String... targetPod) {

    }

    @Override
    public RcDeployment getRCDeployment(TargetResName collection) {
        return null;
    }

    @Override
    public WatchPodLog listPodAndWatchLog(TargetResName collection, String podName, ILogListener listener) {
        return null;
    }

    @Override
    public void setTableStreamHandle(BasicFlinkSourceHandle tableStreamHandle) {

    }
}
