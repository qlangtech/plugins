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

package com.qlangtech.tis.plugin.datax.tdfs.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.exception.DataXException;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.datax.server.FTPServer;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-03 22:43
 **/
public class FtpTDFSLinker extends TDFSLinker {
    private static final String FTP_DISPLAY_NAME = "FTP";
    private static final Logger logger = LoggerFactory.getLogger(FtpTDFSLinker.class);

    @Override
    public <T> T useTdfsSession(TDFSSessionVisitor<T> tdfsSession) {
        return getFTPServer().useFtpHelper(tdfsSession, this);
    }

    @Override
    public ITDFSSession createTdfsSession(Integer timeout) {
        FTPServer ftpServer = getFTPServer();
        return ftpServer.createFtpHelper(timeout, this);
    }

    protected FTPServer getFTPServer() {
        return FTPServer.getServer(this.linker);
    }

    @Override
    public ITDFSSession createTdfsSession() {
        FTPServer ftpServer = getFTPServer();
        return ftpServer.createFtpHelper(ftpServer.timeout, this);
    }


    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return FTP_DISPLAY_NAME;
        }

        public DftDescriptor() {
            super();
        }

        @Override
        protected List<? extends IdentityName> createRefLinkers() {
            return ParamsConfig.getItems(FTPServer.FTP_SERVER);
        }

        @Override
        public boolean validateLinker(IFieldErrorHandler msgHandler, Context context, String fieldName, String linker) {
            return true;
        }

        //        public DftDescriptor() {
//            super();
//            registerSelectOptions(KEY_FTP_SERVER_LINK, () -> ParamsConfig.getItems(FTPServer.FTP_SERVER));
//        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            FtpTDFSLinker ftpLinker = (FtpTDFSLinker) postFormVals.newInstance();
            Descriptor currentRootPluginValidator = AttrValMap.getCurrentRootPluginValidator();

            FTPServer server = FTPServer.getServer(ftpLinker.linker);
            return server.useFtpHelper((ftp) -> {
                try {
                    HashSet<ITDFSSession.Res> allFiles
                            = ftp.getAllFiles(Collections.singletonList(ftpLinker.getRootPath()), 0, 10);
                    if (IDFSReader.class.isAssignableFrom(currentRootPluginValidator.clazz)
                            && CollectionUtils.isEmpty(allFiles)) {
                        msgHandler.addFieldError(context, KEY_FIELD_PATH, "该路径下没有扫描到任何文件，请确认路径是否正确");
                        return false;
                    }
                } catch (DataXException e) {
                    logger.warn(e.getMessage(), e);
                    msgHandler.addFieldError(context, KEY_FIELD_PATH, "路径配置有误，" + e.getMessage());
                    return false;
                }
                return true;
            }, ftpLinker);
        }
    }
}
