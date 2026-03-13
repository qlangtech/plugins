package com.qlangtech.tis.plugin.amazon.s3;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.authtoken.IKerberosUserToken;
import com.qlangtech.tis.config.authtoken.IOffUserToken;
import com.qlangtech.tis.config.authtoken.IUserNamePasswordUserToken;
import com.qlangtech.tis.config.authtoken.IUserTokenVisitor;
import com.qlangtech.tis.config.authtoken.UserToken;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.ClassloaderUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

import static com.qlangtech.tis.plugin.amazon.s3.S3FileSystem.SCHEMA_S3;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/7
 */
public class AmazonS3FileSystemFactory extends FileSystemFactory implements ITISFileSystemFactory {

    private static final Logger logger = LoggerFactory.getLogger(AmazonS3FileSystemFactory.class);

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;
    //  必填：MinIO 服务端点（API 地址）
    @FormField(ordinal = 1, validate = {Validator.require, Validator.url})
    public String endpoint;

    /**
     * 是存储桶名称，相当于一个顶级容器
     */
    @FormField(ordinal = 2, validate = {Validator.require, Validator.identity})
    public String bucket;

    @FormField(ordinal = 3, validate = {Validator.require, Validator.absolute_path})
    public String rootDir;

    //<!-- 可选：如果遇到 region 相关报错，可以设置一个默认 region -->
    @FormField(ordinal = 4, validate = {Validator.identity})
    public String region;

    @FormField(ordinal = 5, validate = {Validator.require})
    public UserToken userToken;

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean pathStyleAccess ;


//    public String accessKey;
//    public String secretKey;

    // @FormField(ordinal = 8, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public final Boolean userHostname = false;

    private transient ITISFileSystem fileSystem;

    @Override
    public ReplayConfiguration getConfiguration() {
        try {
            return ClassloaderUtils.processByResetThreadClassloader(AmazonS3FileSystemFactory.class, () -> {
                final ReplayConfiguration conf = new ReplayConfiguration();
//                try (InputStream input = new ByteArrayInputStream(hdfsSiteContent.getBytes(TisUTF8.get()))) {
//                    conf.addResource(input);
//                }
                // 支持重连？
                conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, -1);
                // conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, false);
                // conf.set(FsPermission.UMASK_LABEL, "000");
                // fs.defaultFS
                conf.set("fs.s3a.impl", org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());

                /**
                 * 显式注册 LocalFileSystem，防止在 TIS 插件 ClassLoader 隔离环境下
                 * S3AFileSystem 创建本地临时文件时找不到 "file" scheme 的实现
                 *  S3AFileSystem 需要本地临时文件：当 Paimon 通过 HadoopFileIO 写入 S3/MinIO 时，S3AFileSystem 内部使用 DiskBlockFactory 先在本地磁盘创建临时文件缓冲数据，然后再上传。创建临时文件时调用了 FileSystem.getLocal(conf)，这需要查找 "file"
                 *   scheme 对应的 LocalFileSystem 实现。
                 */
                conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                // conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hdfsAddress);
                //https://segmentfault.com/q/1010000008473574
                logger.info("userHostname:{}", userHostname);
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
                // cfgProcess.accept(conf);
                // this.setConfiguration(conf);
                if (StringUtils.isNotEmpty(region)) {
                    conf.set("fs.s3a.endpoint.region", region);
                }

                URL endpointUrl = new URL(endpoint);
                conf.set(Constants.ENDPOINT, String.valueOf(endpointUrl));
                conf.setBoolean(Constants.SECURE_CONNECTIONS, "https".equalsIgnoreCase(endpointUrl.getProtocol()));
                conf.setBoolean(Constants.PATH_STYLE_ACCESS, pathStyleAccess);

                userToken.accept(new IUserTokenVisitor<Void>() {
                    @Override
                    public Void visit(IOffUserToken token) throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Void visit(IUserNamePasswordUserToken token) throws Exception {
//                        token.getPassword();
//                        token.getPassword();

                        conf.set(Constants.ACCESS_KEY, token.getUserName());
                        conf.set(Constants.SECRET_KEY, token.getPassword());
                        return null;
                        // throw new UnsupportedOperationException();
                    }

                    @Override
                    public Void visit(IKerberosUserToken token) {
                        throw new UnsupportedOperationException();
                        // SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
//                    ServiceLoader<SecurityInfo> securityload
//                            = ServiceLoader.load(SecurityInfo.class, AmazonS3FileSystemFactory.class.getClassLoader());
//                    List<SecurityInfo> sinfos = new ArrayList<>();
//                    for (SecurityInfo si : securityload) {
//                        sinfos.add(si);
//                    }
//                    SecurityUtil.setSecurityInfoProviders(sinfos.toArray(new SecurityInfo[sinfos.size()]));
//                    cfg.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, token.getKerberosCfg().getPrincipal());
//                    return setConfiguration(token.getKerberosCfg(), fsFactory.getClass(), cfg, () -> create());
                    }
                });


                conf.reloadConfiguration();
                return conf;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void setConfigFile(File cfgDir) {

    }

    @Override
    public String getRootDir() {
        return StringUtils.trimToEmpty(this.rootDir);
    }

    @Override
    public String getFSAddress() {
        boolean rootDirHasSet = StringUtils.isNotEmpty(this.getRootDir());
        return SCHEMA_S3 + "://" + this.bucket + (rootDirHasSet ? (this.getRootDir()) : StringUtils.EMPTY);
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


    private static ITISFileSystem createFS(AmazonS3FileSystemFactory fsFactory) {
        // ClassLoader currLoader = Thread.currentThread().getContextClassLoader();
        try {
            Configuration cfg = fsFactory.getConfiguration();
            final String s3Path = fsFactory.getFSAddress();


            return new S3FileSystem(S3HdfsUtils.getFileSystem(
                    s3Path, cfg), s3Path, fsFactory.getRootDir());

            // Thread.currentThread().setContextClassLoader(HdfsFileSystemFactory.class.getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // Thread.currentThread().setContextClassLoader(currLoader);
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension(ordinal = 0)
    public static class DefaultDescriptor extends Descriptor<FileSystemFactory> implements IEndTypeGetter {
        public DefaultDescriptor() {
            super();
            //  this.registerSelectOptions(IKerberos.IDENTITY, () -> ParamsConfig.getItems(IKerberos.IDENTITY));
        }

        @Override
        public EndType getEndType() {
            return EndType.AmazonS3;
        }

        @Override
        public String getDisplayName() {
            return String.valueOf(getEndType());
        }


//        public boolean validateHdfsSiteContent(IFieldErrorHandler msgHandler
//                , Context context, String fieldName, String value) {
//            try {
//                Configuration cfg = HdfsFileSystemFactory.getConfiguration(value, false, (conf) -> {
//                });
//                String hdfsAddress = cfg.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
//                if (isFSDefaultNameKeyInValid(hdfsAddress)) {
//                    msgHandler.addFieldError(context, KEY_FIELD_HDFS_SITE_CONTENT
//                            , "必须要包含合法的属性'" + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "'");
//                    return false;
//                }
//            } catch (Exception e) {
//                Logger.warn(e.getMessage(), e);
//                msgHandler.addFieldError(context, fieldName, e.getMessage());
//                return false;
//            }
//            return true;
//        }

//        protected boolean isFSDefaultNameKeyInValid(String hdfsAddress) {
//            return StringUtils.isEmpty(hdfsAddress) || StringUtils.startsWith(hdfsAddress, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
//        }


        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // String hdfsAddress = null;
//            try {
//                FileSystemFactory hdfsFactory = postFormVals.newInstance();
//                //Configuration conf = hdfsFactory.getConfiguration();
//                ITISFileSystem hdfs = hdfsFactory.getFileSystem();
//                final IPath rootPath = hdfs.getRootDir();
//                try {
//                    IPathInfo rootInfo = hdfs.getFileInfo(rootPath);
//                    if (!rootInfo.isDir()) {
//                        //   rootDir
//                        msgHandler.addFieldError(context, KEY_ROOT_DIR, "必须为目录");
//                        return false;
//                    }
//                } catch (Exception e) {
//                    Logger.warn(String.valueOf(rootPath), e);
//                    msgHandler.addFieldError(context, KEY_ROOT_DIR, e.getMessage());
//                    return false;
//                }
//                //hdfs.listChildren(hdfs.getPath("/"));
//                msgHandler.addActionMessage(context, "hdfs连接正常");
//                hdfs.close();
//                return true;
//            } catch (Exception e) {
//                Logger.warn(e.getMessage(), e);
//
////                msgHandler.addFieldError(context, KEY_FIELD_HDFS_SITE_CONTENT, "请检查连接地址，服务端是否能正常,"
////                        + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "=" + hdfsAddress + ",错误:" + errMsg.getMessage());
//                processError(msgHandler, context, e);
//                return false;
//            }
            return true;
        }

//        protected void processError(IControlMsgHandler msgHandler, Context context, Exception e) {
//            TisException.ErrMsg errMsg = TisException.getErrMsg(e);
//            msgHandler.addFieldError(context, KEY_FIELD_HDFS_SITE_CONTENT, "请检查连接地址，服务端是否能正常,错误:" + errMsg.getMessage());
//        }

//        public boolean validate() {
//            return true;
//        }
    }
}
