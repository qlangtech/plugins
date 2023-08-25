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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.impl.DataXBasicProcessMeta;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 00:10
 **/
public class DataXDFSReader extends AbstractDFSReader implements DataXBasicProcessMeta.IRDBMSSupport {


    @FormField(ordinal = 8, validate = {Validator.require})
    public FileFormat fileFormat;


    @Override
    public List<DataXDFSReaderWithMeta.TargetResMeta> getSelectedEntities() {
        return this.dfsLinker.useTdfsSession((ftp) -> {
            List<DataXDFSReaderWithMeta.TargetResMeta> ftpFiles = Lists.newArrayList();
            Set<ITDFSSession.Res> allRes = ftp.getListFiles(ftp.getRootPath(), 0, this.resMatcher.maxTraversalLevel);
            DataXDFSReaderWithMeta.TargetResMeta m = null;
            for (ITDFSSession.Res meta : allRes) {
                m = DataXDFSReaderWithMeta.getTargetResMeta(meta);
                if (m != null) {
                    ftpFiles.add(m);
                }
            }
            return ftpFiles;
        });
    }

    @Override
    public FileFormat getFileFormat(Optional<String> entityName) {
        return this.fileFormat;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(AbstractDFSReader.class, "DataXDFSReader-tpl.json");
    }

    @Override
    public boolean isRDBMSSupport() {
        return Objects.requireNonNull(resMatcher, "resMatcher can not be null").isRDBMSSupport();
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
            //    registerSelectOptions(FtpTDFSLinker.KEY_FTP_SERVER_LINK, () -> ParamsConfig.getItems(FTPServer.FTP_SERVER));
        }

        @Override
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.TDFS;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals, true);
        }

        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals, boolean showRecognizedResMsg) {
            AbstractDFSReader dataxReader = (AbstractDFSReader) postFormVals.newInstance(this, msgHandler);

            Set<ITDFSSession.Res> matchRes = dataxReader.dfsLinker.useTdfsSession((session) -> {
                return dataxReader.resMatcher.findAllRes(session);
            });

            if (CollectionUtils.isEmpty(matchRes)) {
                msgHandler.addFieldError(context, KEY_RES_MATCHER, "路径:" + dataxReader.dfsLinker.getRootPath() + "下，使用匹配:'" + dataxReader.resMatcher + "'不能找到对应的资源文件");
                return false;
            } else {
                if (showRecognizedResMsg) {
                    int count = 0;
                    msgHandler.addActionMessage(context, "找个" + matchRes.size() + "份资源文件");
                    for (ITDFSSession.Res res : matchRes) {
                        msgHandler.addActionMessage(context, res.fullPath);
                        if (count++ > 4) {
                            break;
                        }
                    }
                }
            }
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals, false);
        }

        @Override
        public boolean isRDBMSChangeableInLifetime() {
            return true;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return this.getEndType().name();
        }
    }
}
