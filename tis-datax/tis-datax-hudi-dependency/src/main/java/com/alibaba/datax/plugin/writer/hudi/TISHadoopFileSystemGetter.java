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

package com.alibaba.datax.plugin.writer.hudi;

import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.IExtraHadoopFileSystemGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-02 12:11
 **/
public class TISHadoopFileSystemGetter implements IExtraHadoopFileSystemGetter {
    private Configuration configuration;
    private static final Logger LOG = LoggerFactory.getLogger(TISHadoopFileSystemGetter.class);
    static boolean initializeDir = false;

    @Override
    public FileSystem getHadoopFileSystem(String path) {

        try {
            if (!initializeDir) {
                // 初始化过程会在spark远端执行，此时dataDir可能还没有初始化，需要有一个初始化目录的过程
                File dataDir = Config.getDataDir(false);
                try {
                    FileUtils.forceMkdir(dataDir);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                //  try {
                // Integer taskId = Integer.parseInt(System.getenv(IParamContext.KEY_TASK_ID));
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                URL resource = classLoader.getResource(PluginAndCfgsSnapshot.getTaskEntryName());
                resource = new URL(StringUtils.substringBefore(resource.getFile(), "!"));
//                File mainifest = new File(().toURI());
//                if (!mainifest.exists()) {
//                    throw new IllegalStateException("mainifest file can is not exist:" + mainifest.getAbsolutePath());
//                }

                try (InputStream mainifest = resource.openStream()) {
                    PluginAndCfgsSnapshot remoteSnapshot
                            = PluginAndCfgsSnapshot.getRepositoryCfgsSnapshot(resource.toString(), mainifest);
                    PluginAndCfgsSnapshot localSnaphsot = PluginAndCfgsSnapshot.getLocalPluginAndCfgsSnapshot(remoteSnapshot.getAppName(), Optional.empty());
                    remoteSnapshot.synchronizTpisAndConfs(localSnaphsot);
                }


                // System.out.println("dddddd:" + resource);
                // LOG.info("dddddddddddddddddddd:" + resource);
                // throw new IllegalStateException("dddddddddddddddddddd:" + resource);
                //  initializeDir = true;
                //            } catch (Exception ee) {
                //                Map<String, String> getenv = System.getenv();
                //                Properties properties = System.getProperties();
                //
                //                throw new RuntimeException(getenv.entrySet().stream().map((e) -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",\n")) +
                //                         "system props:\n" +
                //                        properties.entrySet().stream().map((e) -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",\n"))
                //                        , ee);
                //            }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        FileSystemFactory fsFactory = FileSystemFactory.getFsFactory(HUDI_FILESYSTEM_NAME);
        if (fsFactory == null) {
            throw new IllegalStateException(
                    "fs identity:" + HUDI_FILESYSTEM_NAME + " relevant fileSystemFactory can not be null");
        }
        if (configuration == null) {
            synchronized (TISHadoopFileSystemGetter.class) {
                if (configuration == null) {
                    configuration = fsFactory.getConfiguration();
                }
            }
        }
        try {
            return new Path(path).getFileSystem(configuration);
        } catch (ClassCastException e) {
            throw new RuntimeException(Configuration.class.getClassLoader()
                    + ",cast from:" + fsFactory.getConfiguration().getClass().getClassLoader(), e);
        } catch (IOException e) {
            throw new RuntimeException("path:" + path, e);
        }
    }
}
