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
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.BasicPainFormat.BasicPainFormatDescriptor;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.datax.resmatcher.MetaAwareDFSResMatcher;
import com.qlangtech.tis.plugin.datax.resmatcher.WildcardDFSResMatcher;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.IExclusiveTDFSType;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 00:10
 **/
public class DataXDFSReader extends AbstractDFSReader implements DataXBasicProcessMeta.IRDBMSSupport  {


    @FormField(ordinal = 8, validate = {Validator.require})
    public FileFormat fileFormat;


    public static List<? extends Descriptor> dfsLinkerFilter(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            return Collections.emptyList();
        }
        return descs.stream().filter((desc) -> {
            return !(desc instanceof IExclusiveTDFSType);
        }).collect(Collectors.toList());
    }

    public static List<? extends Descriptor> supportedReaderFormat(List<? extends Descriptor> descs) {
        return BasicPainFormatDescriptor.supportedFormat(true, descs);
    }
    @Override
    public boolean isRDBMSSupport() {
        return super.isRDBMSSupport();
    }

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
    public ThreadCacheTableCols getContextTableColsStream(SuFormProperties.SuFormGetterContext context) {
        if (this.resMatcher instanceof MetaAwareDFSResMatcher) {
            return super.getContextTableColsStream(context);
        } else if (this.resMatcher instanceof WildcardDFSResMatcher) {
            return this.getContextTableColsStream(context, (targetTab) -> {
                try {
                    List<ColumnMetaData> tableMetadata = getColumnMetaData(context.param.getPluginContext(), targetTab);
                    return tableMetadata.stream().map((col) -> ColumnMetaData.convert(col)).collect(Collectors.toList());
                } catch (TableNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            throw new IllegalStateException("invalid resMatcher class:" + this.resMatcher.getClass().getSimpleName());
        }
    }

    private List<ColumnMetaData> getColumnMetaData(IPluginContext context, EntityName targetTab) throws TableNotFoundException {
        List<ColumnMetaData> tableMetadata
                = this.resMatcher.getTableMetadata(
                context, this.dataXName, this, targetTab);
        return tableMetadata;
    }

    @Override
    public List<SelectedTab> fillSelectedTabMeta(List<SelectedTab> tabs) {
        if (this.resMatcher instanceof MetaAwareDFSResMatcher) {
            return tabs.stream().map((tab) -> {
                ColumnMetaData.fillSelectedTabMeta(tab, (t) -> {
                    try {
                        return getColumnMetaData(IPluginContext.getThreadLocalInstance(), tab.getEntityName())
                                .stream().collect(Collectors.toMap(Option::getName, (col) -> col));
                    } catch (TableNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                });
                return tab;
            }).collect(Collectors.toList());

        }
        return tabs;
    }




    @Override
    public FileFormat getFileFormat(Optional<String> entityName) {
        return this.fileFormat;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(AbstractDFSReader.class, "DataXDFSReader-tpl.json");
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
            AbstractDFSReader dataxReader = postFormVals.newInstance();

            //try {
            //
            /**
             * 当reader header读取，但是在head 无法正确读取到colName 则在此处会抛异常
             * @see CSVFormat#setCsvReaderConfig 会抛出TisException
             */
//                dataxReader.getSelectedTabs();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }

            Set<ITDFSSession.Res> matchRes = dataxReader.dfsLinker.useTdfsSession((session) -> {
                return dataxReader.resMatcher.findAllRes(session);
            });

            if (CollectionUtils.isEmpty(matchRes)) {
                msgHandler.addFieldError(context, KEY_RES_MATCHER, "路径:"
                        + dataxReader.dfsLinker.getRootPath() + "下，使用匹配:'" + dataxReader.resMatcher + "'不能找到对应的资源文件");
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
