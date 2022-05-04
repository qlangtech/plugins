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
package com.qlangtech.tis.hdfs.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2018年11月23日
 */
@Public
public class HdfsFileSystemFactory extends FileSystemFactory implements ITISFileSystemFactory {

    private static final Logger Logger = LoggerFactory.getLogger(HdfsFileSystemFactory.class);

    private static final String KEY_FIELD_HDFS_ADDRESS = "hdfsAddress";

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean userHostname;

    @FormField(ordinal = 4, validate = {Validator.require, Validator.url})
    public String hdfsAddress;

    @FormField(ordinal = 7, validate = {Validator.require, Validator.absolute_path})
    public String rootDir;

    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String hdfsSiteContent;

    private ITISFileSystem fileSystem;

    @Override
    public String identityValue() {
        return this.name;
    }

    @Override
    public ITISFileSystem getFileSystem() {
        if (fileSystem == null) {
            fileSystem = new HdfsFileSystem(HdfsUtils.getFileSystem(
                    hdfsAddress, getConfiguration()), hdfsAddress, this.rootDir);
        }
        return fileSystem;
    }

    @Override
    public Configuration getConfiguration() {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HdfsFileSystemFactory.class.getClassLoader());
            Configuration conf = new Configuration();
            try (InputStream input = new ByteArrayInputStream(hdfsSiteContent.getBytes(TisUTF8.get()))) {
                conf.addResource(input);
            }
            conf.set(FsPermission.UMASK_LABEL, "000");
            // fs.defaultFS
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hdfsAddress);
            //https://segmentfault.com/q/1010000008473574
            Logger.info("userHostname:{}", userHostname);
            if (userHostname != null && userHostname) {
                conf.set("dfs.client.use.datanode.hostname", "true");
            }

            conf.set("fs.defaultFS", hdfsAddress);
            conf.set("hadoop.job.ugi", "admin");
            // 这个缓存还是需要的，不然如果另外的调用FileSystem实例不是通过调用getFileSystem这个方法的进入,就调用不到了
            conf.setBoolean("fs.hdfs.impl.disable.cache", false);
            conf.reloadConfiguration();
            return conf;
        } catch (Exception e) {
            throw new RuntimeException("hdfsAddress:" + hdfsAddress, e);
        }finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Override
    public String getFSAddress() {
        return this.hdfsAddress;
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

        public static FileSystem getFileSystem(String hdfsAddress, Configuration config) {

            FileSystem fileSystem = fileSys.get(hdfsAddress);
            if (fileSystem == null) {
                synchronized (HdfsUtils.class) {


                    try {


                        fileSystem = fileSys.get(hdfsAddress);
                        if (fileSystem == null) {
//                            Configuration conf = new Configuration();
//                            conf.set(FsPermission.UMASK_LABEL, "000");
//                            // fs.defaultFS
//                            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//                            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hdfsAddress);
//                            //https://segmentfault.com/q/1010000008473574
//                            Logger.info("userHostname:{}", userHostname);
//                            if (userHostname != null && userHostname) {
//                                conf.set("dfs.client.use.datanode.hostname", "true");
//                            }
//
//                            conf.set("fs.default.name", hdfsAddress);
//                            conf.set("hadoop.job.ugi", "admin");
//                            try (InputStream input = new ByteArrayInputStream(hdfsContent.getBytes(TisUTF8.get()))) {
//                                conf.addResource(input);
//                                // 这个缓存还是需要的，不然如果另外的调用FileSystem实例不是通过调用getFileSystem这个方法的进入,就调用不到了
//                                conf.setBoolean("fs.hdfs.impl.disable.cache", false);
                            fileSystem = new FilterFileSystem(FileSystem.get(config)) {
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
                                public FSDataOutputStream create(Path f, FsPermission permission
                                        , boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
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
                            Logger.info("successful create hdfs with hdfsAddress:" + hdfsAddress);
                            fileSys.put(hdfsAddress, fileSystem);
                        }

                    } catch (Throwable e) {
                        throw new RuntimeException("link faild:" + hdfsAddress, e);
                    } finally {

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
            return "HDFS";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            ParseDescribable<Describable> fs = null;
            try {
               // fs =
                FileSystemFactory hdfsFactory = postFormVals.newInstance(this, msgHandler);// this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
              //  HdfsFileSystemFactory hdfsFactory = (HdfsFileSystemFactory) fs.getInstance();
                ITISFileSystem hdfs = hdfsFactory.getFileSystem();
                hdfs.listChildren(hdfs.getPath("/"));
                msgHandler.addActionMessage(context, "hdfs连接:" + ((HdfsFileSystemFactory) fs.getInstance()).hdfsAddress + "连接正常");
                return true;
            } catch (Exception e) {
                Logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, KEY_FIELD_HDFS_ADDRESS, "请检查连接地址，服务端是否能正常,错误:" + e.getMessage());
                return false;
            }
        }

        public boolean validate() {
            return true;
        }
    }
}

