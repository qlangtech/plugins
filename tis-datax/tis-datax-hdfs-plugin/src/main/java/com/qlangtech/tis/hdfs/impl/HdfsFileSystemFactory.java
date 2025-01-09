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
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.Utils;
import com.qlangtech.tis.config.authtoken.*;
import com.qlangtech.tis.config.kerberos.IKerberos;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.ClassloaderUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2018年11月23日
 */
@Public
public class HdfsFileSystemFactory extends FileSystemFactory implements ITISFileSystemFactory {

    private static final Logger Logger = LoggerFactory.getLogger(HdfsFileSystemFactory.class);
    private static final String KEY_ROOT_DIR = "rootDir";
    // private static final String KEY_FIELD_HDFS_ADDRESS = "hdfsAddress";
    private static final String KEY_FIELD_HDFS_SITE_CONTENT = "hdfsSiteContent";

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public Boolean userHostname;

//    @FormField(ordinal = 4, validate = {Validator.require, Validator.url})
//    public String hdfsAddress;

    @FormField(ordinal = 7, validate = {Validator.require, Validator.absolute_path})
    public String rootDir;

//    @FormField(ordinal = 8, type = FormFieldType.SELECTABLE, advance = true, validate = {})
//    public String kerberos;

    @FormField(ordinal = 8, validate = {Validator.require})
    public UserToken userToken;


    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String hdfsSiteContent;

    private ITISFileSystem fileSystem;

    @Override
    public String identityValue() {
        return this.name;
    }

    public static String dftHdfsSiteContent() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <property>\n" +
                "        <name>" + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "</name>\n" +
                "        <value></value>\n" +
                "    </property>\n" +
                "</configuration> ";
    }

    @Override
    public ITISFileSystem getFileSystem() {
        try {
            if (fileSystem == null) {
                this.fileSystem = createFS(this);
            }
            return fileSystem;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ITISFileSystem createFS(HdfsFileSystemFactory fsFactory) {
        // ClassLoader currLoader = Thread.currentThread().getContextClassLoader();
        try {
            Configuration cfg = fsFactory.getConfiguration();
            String hdfsAddress = fsFactory.getFSAddress();
            // Thread.currentThread().setContextClassLoader(HdfsFileSystemFactory.class.getClassLoader());
            return fsFactory.userToken.accept(new IUserTokenVisitor<ITISFileSystem>() {
                @Override
                public ITISFileSystem visit(IOffUserToken token) throws Exception {
                    final Thread t = Thread.currentThread();
                    final ClassLoader contextClassLoader = t.getContextClassLoader();
                    try {
                        t.setContextClassLoader(fsFactory.getClass().getClassLoader());
                        cfg.setClassLoader(fsFactory.getClass().getClassLoader());
                        UserGroupInformation.setConfiguration(cfg);
                        return create();
                    } finally {
                        t.setContextClassLoader(contextClassLoader);
                    }
                }

                @Override
                public ITISFileSystem visit(IUserNamePasswordUserToken token) throws Exception {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ITISFileSystem visit(IKerberosUserToken token) {
                    // SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
                    ServiceLoader<SecurityInfo> securityload
                            = ServiceLoader.load(SecurityInfo.class, HdfsFileSystemFactory.class.getClassLoader());
                    List<SecurityInfo> sinfos = new ArrayList<>();
                    for (SecurityInfo si : securityload) {
                        sinfos.add(si);
                    }
                    SecurityUtil.setSecurityInfoProviders(sinfos.toArray(new SecurityInfo[sinfos.size()]));
                    cfg.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, token.getKerberosCfg().getPrincipal());
                    return setConfiguration(token.getKerberosCfg(), fsFactory.getClass(), cfg, () -> create());
                }

                private final ITISFileSystem create() {
                    return new HdfsFileSystem(HdfsUtils.getFileSystem(
                            hdfsAddress, cfg), hdfsAddress, fsFactory.rootDir);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // Thread.currentThread().setContextClassLoader(currLoader);
        }
    }

    @Override
    public void setConfigFile(File cfgDir) {
        Utils.setHadoopConfig2Local(cfgDir, "hdfs-site.xml", hdfsSiteContent);
    }


    public static List<? extends Descriptor> filter(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            throw new IllegalArgumentException("param descs can not be null");
        }
        return descs.stream().filter((d) -> {
            return !IUserNamePasswordUserToken.KEY_USER_PASS.equals(((Descriptor) d).getDisplayName());
        }).collect(Collectors.toList());
    }

    private static Configuration getConfiguration(String hdfsSiteContent, Boolean userHostname, Consumer<Configuration> cfgProcess) {


//        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
//        try {
//            Thread.currentThread().setContextClassLoader(HdfsFileSystemFactory.class.getClassLoader());
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            Thread.currentThread().setContextClassLoader(contextClassLoader);
//        }


        try {
            return ClassloaderUtils.processByResetThreadClassloader(HdfsFileSystemFactory.class, () -> {
                Configuration conf = new Configuration();
                try (InputStream input = new ByteArrayInputStream(hdfsSiteContent.getBytes(TisUTF8.get()))) {
                    conf.addResource(input);
                }
                // 支持重连？
                conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, -1);
                // conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, false);
                conf.set(FsPermission.UMASK_LABEL, "000");
                // fs.defaultFS
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                // conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hdfsAddress);
                //https://segmentfault.com/q/1010000008473574
                Logger.info("userHostname:{}", userHostname);
                if (userHostname != null && userHostname) {
                    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME, true);
                }

                // conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsAddress);
                conf.set("hadoop.job.ugi", "admin");
                // 这个缓存还是需要的，不然如果另外的调用FileSystem实例不是通过调用getFileSystem这个方法的进入,就调用不到了
                conf.setBoolean("fs.hdfs.impl.disable.cache", false);

    //            if (StringUtils.isNotEmpty(this.kerberos)) {
    //                Logger.info("kerberos has been enabled,name:" + this.kerberos);
    //                KerberosCfg kerberosCfg = KerberosCfg.getKerberosCfg(this.kerberos);
    //                kerberosCfg.setConfiguration(conf);
    //            }
                cfgProcess.accept(conf);
                // this.setConfiguration(conf);
                conf.reloadConfiguration();
                return conf;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConfiguration() {
        return getConfiguration(this.hdfsSiteContent, this.userHostname, (conf) -> setConfiguration(conf));
    }

    protected void setConfiguration(Configuration config) {

    }


    public static <T> T setConfiguration(
            IKerberos kerberos, Class<?> ownerClass, final Configuration config, Supplier<T> creator) {
        Objects.requireNonNull(config, "config can not be null");
        Objects.requireNonNull(ownerClass, "ownerClass can not be null");
//        if (!(config instanceof Configuration)) {
//            throw new IllegalArgumentException("param config must be type of Configuration, but now is :" + config.getClass().getName());
//        }

        final Thread t = Thread.currentThread();
        final ClassLoader contextClassLoader = t.getContextClassLoader();
        if (StringUtils.isEmpty(kerberos.getPrincipal())) {
            throw new IllegalStateException("prop principal can not be null");
        }
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, config);
        File keytab = kerberos.getKeyTabPath();
        if (!keytab.exists()) {
            throw new IllegalStateException("keytabPath can is not exist:" + kerberos.getKeytabPath());
        }
        try {
            t.setContextClassLoader(ownerClass.getClassLoader());
            config.setClassLoader(ownerClass.getClassLoader());
            UserGroupInformation.setConfiguration((Configuration) config);
            UserGroupInformation.loginUserFromKeytab(kerberos.getPrincipal(), keytab.getAbsolutePath());
            return creator.get();
        } catch (IOException e) {
            throw new RuntimeException("principal:" + kerberos.getPrincipal(), e);
        } finally {
            t.setContextClassLoader(contextClassLoader);
        }
    }

    @Override
    public String getFSAddress() {
        return getConfiguration().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    }
//
//    public void setHdfsAddress(String hdfsAddress) {
//        this.hdfsAddress = hdfsAddress;
//    }

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

//                                    if (f.getName().indexOf("hoodie.properties") > -1) {
//                                        throw new IllegalStateException("not support:" + f.getName());
//                                    }

                                    return super.create(f, FsPermission.getDefault(), overwrite, bufferSize, replication, blockSize, progress);
                                }

//                                @Override
//                                public FileStatus[] listStatus(Path f) throws IOException {
//                                    return super.listStatus(f);
//                                }

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
                        // throw new RuntimeException("link faild:" + hdfsAddress, e);
                        throw TisException.create("link faild:" + hdfsAddress + ",detail:" + e.getMessage(), e);
                    } finally {

                    }
                }

            }
            return fileSystem;
        }


    }


    @TISExtension(ordinal = 0)
    public static class DefaultDescriptor extends Descriptor<FileSystemFactory> {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(IKerberos.IDENTITY, () -> ParamsConfig.getItems(IKerberos.IDENTITY));
        }

        @Override
        public String getDisplayName() {
            return "HDFS";
        }


        public boolean validateHdfsSiteContent(IFieldErrorHandler msgHandler
                , Context context, String fieldName, String value) {
            try {
                Configuration cfg = HdfsFileSystemFactory.getConfiguration(value, false, (conf) -> {
                });
                String hdfsAddress = cfg.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
                if (isFSDefaultNameKeyInValid(hdfsAddress)) {
                    msgHandler.addFieldError(context, KEY_FIELD_HDFS_SITE_CONTENT
                            , "必须要包含合法的属性'" + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "'");
                    return false;
                }
            } catch (Exception e) {
                Logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }

        protected boolean isFSDefaultNameKeyInValid(String hdfsAddress) {
            return StringUtils.isEmpty(hdfsAddress) || StringUtils.startsWith(hdfsAddress, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        }


        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // String hdfsAddress = null;
            try {
                FileSystemFactory hdfsFactory = postFormVals.newInstance();
                //Configuration conf = hdfsFactory.getConfiguration();
                ITISFileSystem hdfs = hdfsFactory.getFileSystem();
                final IPath rootPath = hdfs.getRootDir();
                try {
                    IPathInfo rootInfo = hdfs.getFileInfo(rootPath);
                    if (!rootInfo.isDir()) {
                        //   rootDir
                        msgHandler.addFieldError(context, KEY_ROOT_DIR, "必须为目录");
                        return false;
                    }
                } catch (Exception e) {
                    Logger.warn(String.valueOf(rootPath), e);
                    msgHandler.addFieldError(context, KEY_ROOT_DIR, e.getMessage());
                    return false;
                }
                //hdfs.listChildren(hdfs.getPath("/"));
                msgHandler.addActionMessage(context, "hdfs连接正常");
                hdfs.close();
                return true;
            } catch (Exception e) {
                Logger.warn(e.getMessage(), e);

//                msgHandler.addFieldError(context, KEY_FIELD_HDFS_SITE_CONTENT, "请检查连接地址，服务端是否能正常,"
//                        + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "=" + hdfsAddress + ",错误:" + errMsg.getMessage());
                processError(msgHandler, context, e);
                return false;
            }
        }

        protected void processError(IControlMsgHandler msgHandler, Context context, Exception e) {
            TisException.ErrMsg errMsg = TisException.getErrMsg(e);
            msgHandler.addFieldError(context, KEY_FIELD_HDFS_SITE_CONTENT, "请检查连接地址，服务端是否能正常,错误:" + errMsg.getMessage());
        }

//        public boolean validate() {
//            return true;
//        }
    }
}

