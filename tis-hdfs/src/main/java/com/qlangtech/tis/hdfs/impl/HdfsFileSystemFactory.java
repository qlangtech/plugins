/**
 * Copyright 2020 QingLang, Inc.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2018年11月23日
 */
public class HdfsFileSystemFactory extends FileSystemFactory implements ITISFileSystemFactory {

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.url})
    public String hdfsAddress;

    @FormField(ordinal = 2, validate = {Validator.require})
    public String rootDir;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {Validator.require})
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

        private static final Map<String, FileSystem> fileSys = new HashMap<String, FileSystem>();

        public static FileSystem getFileSystem(String hdfsAddress, String hdfsContent) {

            FileSystem fileSystem = fileSys.get(hdfsAddress);
            if (fileSystem == null) {
                synchronized (HdfsUtils.class) {

                    final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(HdfsFileSystemFactory.class.getClassLoader());

                        fileSystem = fileSys.get(hdfsAddress);
                        if (fileSystem == null) {
                            Configuration conf = new Configuration();
                            conf.set(FsPermission.UMASK_LABEL, "000");
                            // fs.defaultFS
                            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
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
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    } finally {
                        Thread.currentThread().setContextClassLoader(contextClassLoader);
                    }
                }

            }
            return fileSystem;
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

