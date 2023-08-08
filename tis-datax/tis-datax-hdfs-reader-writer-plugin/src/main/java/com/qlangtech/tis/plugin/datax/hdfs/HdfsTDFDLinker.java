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

package com.qlangtech.tis.plugin.datax.hdfs;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 22:25
 **/
public class HdfsTDFDLinker extends TDFSLinker {
    public static final String DATAX_NAME = "Hdfs";
    private static final Logger logger = LoggerFactory.getLogger(HdfsTDFDLinker.class);
    private HdfsFileSystemFactory fileSystem = null;

    @Override
    public ITDFSSession createTdfsSession(Integer timeout) {
        return createTdfsSession();
    }

    @Override
    public String getRootPath() {
        ITISFileSystem fs = getFs().getFileSystem();
        return String.valueOf(fs.getPath(fs.getPath(getFs().rootDir), this.path));
    }

    public HdfsFileSystemFactory getFs() {
        if (fileSystem == null) {
            this.fileSystem = (HdfsFileSystemFactory) FileSystemFactory.getFsFactory(this.linker);
        }
        Objects.requireNonNull(this.fileSystem, "fileSystem has not be initialized");
        return fileSystem;
    }

    @Override
    public ITDFSSession createTdfsSession() {
        return new HdfsTDFSSession(this);
    }

    @Override
    public <T> T useTdfsSession(TDFSSessionVisitor<T> tdfsSession) {
        try {
            return tdfsSession.accept(createTdfsSession());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        protected List<? extends IdentityName> createRefLinkers() {
            return TIS.getPluginStore(FileSystemFactory.class)
                    .getPlugins().stream().filter(((f) -> f instanceof HdfsFileSystemFactory)).collect(Collectors.toList());
        }

        @Override
        public boolean validateLinker(IFieldErrorHandler msgHandler, Context context, String fieldName, String endpoint) {
//            AliyunEndpoint end = IHttpToken.getAliyunEndpoint(endpoint);
//            return end.accept(new AuthToken.Visitor<Boolean>() {
//                @Override
//                public Boolean visit(NoneToken noneToken) {
//                    // Validator.require.validate(msgHandler, context, fieldName, null);
//                    msgHandler.addFieldError(context, fieldName, "请填写AccessKey/AccessToken");
//                    return false;
//                }
//
//                @Override
//                public Boolean visit(AccessKey accessKey) {
//                    return true;
//                }
//
//                @Override
//                public Boolean visit(UsernamePassword accessKey) {
//                    msgHandler.addFieldError(context, fieldName, "不支持使用用户名/密码认证方式");
//                    return false;
//                }
//            });
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            HdfsTDFDLinker dfsLinker = (HdfsTDFDLinker) postFormVals.newInstance(this, msgHandler);
            return verifyForm(msgHandler, context, dfsLinker);
        }

        private static boolean verifyForm(IControlMsgHandler msgHandler, Context context, HdfsTDFDLinker dfsLinker) {
            ITISFileSystem fs = null;
            try {
                fs = dfsLinker.getFs().getFileSystem();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, KEY_FTP_SERVER_LINK, e.getMessage());
                return false;
            }
            IPath rootPath = fs.getPath(dfsLinker.getRootPath());
            try {
                //  IPath rootPath = fs.getPath(dfsLinker.getRootPath());
                IPathInfo pathInfo = fs.getFileInfo(rootPath);
                if (!pathInfo.isDir()) {
                    msgHandler.addFieldError(context, KEY_FIELD_PATH, "路径:" + rootPath + "不是目录");
                    return false;
                }
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, KEY_FIELD_PATH, "路径不存在：" + rootPath);
                return false;
            }

            return true;
        }
    }
}
