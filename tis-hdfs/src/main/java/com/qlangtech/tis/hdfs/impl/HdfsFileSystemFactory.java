/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.hdfs.impl;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.manage.common.TisUTF8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2018年11月23日
 */
public class HdfsFileSystemFactory extends FileSystemFactory implements ITISFileSystemFactory {

    @FormField(require = true, ordinal = 0, validate = { Validator.require, Validator.identity })
    public String name;

    @FormField(require = true, ordinal = 1, validate = { Validator.require, Validator.host })
    public String hdfsAddress;

    @FormField(require = true, ordinal = 2, validate = { Validator.require })
    public String rootDir;

    @FormField(require = true, ordinal = 3, type = FormFieldType.TEXTAREA, validate = { Validator.require })
    public String hdfsSiteContent;

    private ITISFileSystem fileSystem;

    @Override
    public ITISFileSystem getFileSystem() {
        if (fileSystem == null) {
            fileSystem = new HdfsFileSystem(HdfsUtils.getFileSystem(hdfsAddress, hdfsSiteContent));
        }
        return fileSystem;
    }

    @Override
    public String getRootDir() {
        return this.rootDir;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getHdfsAddress() {
        return hdfsAddress;
    }

    public void setHdfsAddress(String hdfsAddress) {
        this.hdfsAddress = hdfsAddress;
    }

    public String getHdfsSiteContent() {
        return hdfsSiteContent;
    }

    public void setHdfsSiteContent(String hdfsSiteContent) {
        this.hdfsSiteContent = hdfsSiteContent;
    }

    private static class HdfsUtils {

        // private static final Logger logger = LoggerFactory.getLogger(HdfsUtils.class);
        // /**
        // * @param
        // * @param
        // * @param
        // * @return
        // */
        // public static List<Path> getLibPaths(final String localJarDir, Path dest /* hdfs 目标目录 */) {
        // try {
        // // String localJarDir = null;
        // // if (commandLine != null) {
        // // localJarDir =
        // // commandLine.getOptionValue(YarnConstant.PARAM_OPTION_LOCAL_JAR_DIR);
        // // }
        // 
        // List<Path> libs = null;
        // if (StringUtils.isNotBlank(localJarDir)) {
        // libs = copyLibs2Hdfs(localJarDir, dest);
        // } else {
        // libs = new ArrayList<Path>();
        // // new Path(YarnConstant.HDFS_GROUP_LIB_DIR + "/" +
        // // runtime.getKeyName())
        // 
        // if (!getFileSystem().exists(dest)) {
        // throw new IllegalStateException("target dest:" + dest
        // + " is not exist ,please make sure having deploy the index build jar to hdfs.");
        // }
        // 
        // for (FileStatus s : getFileSystem().listStatus(dest)) {
        // libs.add(s.getPath());
        // }
        // }
        // if (libs.size() < 1) {
        // throw new IllegalStateException("libs size can not small than 1");
        // }
        // return libs;
        // } catch (Exception e) {
        // throw new RuntimeException(e);
        // }
        // }
        /**
         * 将本地jar包上传到hdfs上去
         *
         * @param localJarDir
         * @param
         * @return
         * @throws Exception
         */
        // public static List<Path> copyLibs2Hdfs(String localJarDir, Path dest /* hdfs 目标目录 */) throws Exception {
        // List<Path> libs = new ArrayList<Path>();
        // if (StringUtils.isBlank(localJarDir)) {
        // throw new IllegalArgumentException("param localJarDir can not be null");
        // }
        // 
        // // 本地删除
        // FileSystem fs = getFileSystem();
        // // final Path path = new Path(YarnConstant.HDFS_GROUP_LIB_DIR + "/" +
        // // runtime.getKeyName());
        // fs.delete(dest, true);
        // logger.info("path:" + dest + " have been delete");
        // 
        // // 取得需要的lib包
        // File dir = new File(localJarDir);
        // String[] childs = null;
        // if (!dir.isDirectory() || (childs = dir.list(new FilenameFilter() {
        // @Override
        // public boolean accept(File dir, String name) {
        // return StringUtils.endsWith(name, ".jar");
        // }
        // })).length < 1) {
        // throw new IllegalStateException("dir:" + dir.getAbsolutePath() + " has not find any jars");
        // }
        // 
        // URI source = null;
        // Path d = null;
        // for (String f : childs) {
        // source = (new File(dir, f)).toURI();
        // d = new Path(dest, f);
        // fs.copyFromLocalFile(new Path(source), d);
        // libs.add(d);
        // logger.info("local:" + source + " have been copy to hdfs");
        // }
        // 
        // return libs;
        // }
        private static final Map<String, FileSystem> fileSys = new HashMap<String, FileSystem>();

        // public static FileSystem getFileSystem() {
        // TSearcherConfigFetcher config = TSearcherConfigFetcher.get();
        // return getFileSystem(config.getHdfsAddress());
        // }
        public static FileSystem getFileSystem(String hdfsAddress, String hdfsContent) {
            try {
                FileSystem fileSystem = fileSys.get(hdfsAddress);
                if (fileSystem == null) {
                    synchronized (HdfsUtils.class) {
                        fileSystem = fileSys.get(hdfsAddress);
                        if (fileSystem == null) {
                            Configuration conf = new Configuration();
                            conf.set(FsPermission.UMASK_LABEL, "000");
                            // fs.defaultFS
                            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hdfsAddress);
                            conf.set("fs.default.name", hdfsAddress);
                            conf.set("hadoop.job.ugi", "admin");
                            try (InputStream input = new ByteArrayInputStream(hdfsContent.getBytes(TisUTF8.get()))) {
                                conf.addResource(input);
                                // conf.set("dfs.nameservices", "cluster-cdh");
                                // conf.set("dfs.client.failover.proxy.provider.cluster-cdh",
                                // "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                                // //////////////
                                // conf.set("dfs.ha.automatic-failover.enabled.cluster-cdh",
                                // "true");
                                // conf.set("dfs.ha.automatic-failover.enabled.cluster-cdh",
                                // "true");
                                conf.setBoolean("fs.hdfs.impl.disable.cache", true);
                                fileSystem = new FilterFileSystem(FileSystem.get(conf)) {

                                    @Override
                                    public boolean delete(Path f, boolean recursive) throws IOException {
                                        try {
                                            return super.delete(f, recursive);
                                        } catch (Exception e) {
                                            throw new RuntimeException("path:" + f, e);
                                        }
                                    }

                                    @Override
                                    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
                                        return super.mkdirs(f, FsPermission.getDirDefault());
                                    }

                                    @Override
                                    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
                                        return super.create(f, FsPermission.getDefault(), overwrite, bufferSize, replication, blockSize, progress);
                                    }

                                    @Override
                                    public FileStatus[] listStatus(Path f) throws IOException {
                                        try {
                                            return super.listStatus(f);
                                        } catch (Exception e) {
                                            throw new RuntimeException("path:" + f, e);
                                        }
                                    }

                                    @Override
                                    public void close() throws IOException {
                                    // super.close();
                                    // 设置不被关掉
                                    }
                                };
                                fileSystem.listStatus(new Path("/"));
                                fileSys.put(hdfsAddress, fileSystem);
                            }
                        }
                    }
                }
                return fileSystem;
            } catch (IOException e) {
                throw new RuntimeException("hdfsAddress:" + hdfsAddress, e);
            }
        }
    }

    @TISExtension(ordinal = 0)
    public static class DefaultDescriptor extends Descriptor<FileSystemFactory> {

        @Override
        public String getDisplayName() {
            return "hdfs";
        }

        public boolean validate() {
            return true;
        }
    }
}
