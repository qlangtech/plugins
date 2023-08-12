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
import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.datax.server.FTPServer;
import com.qlangtech.tis.plugin.datax.tdfs.impl.FtpTDFSLinker;
import com.qlangtech.tis.plugin.ds.DBIdentity;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.ftpreader.FtpReader
 **/
@Public
public class DataXDFSReader extends DataxReader {
    private static final Logger logger = LoggerFactory.getLogger(DataXDFSReader.class);
    public static final String KEY_DFS_LINKER = "dfsLinker";
    public static final String KEY_RES_MATCHER = "resMatcher";


    @FormField(ordinal = 1, validate = {Validator.require})
    public TDFSLinker dfsLinker;

    @FormField(ordinal = 2, validate = {Validator.require})
    public DFSResMatcher resMatcher;

//    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
//    public String linker;
//
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
//    public String path;

    @FormField(ordinal = 8, validate = {Validator.require})
    public FileFormat fileFormat;

//    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String column;

//    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String compress;
//    @FormField(ordinal = 11, type = FormFieldType.ENUM, validate = {})
//    public String encoding;
    //    @FormField(ordinal = 12, type = FormFieldType.ENUM, validate = {})
//    public Boolean skipHeader;
//    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, validate = {})
//    public String nullFormat;


    public static List<Option> supportCompress() {
        return Arrays.stream(Compress.values()).map((c) -> new Option(c.name(), c.token)).collect(Collectors.toList());
    }

    @Override
    public final TableInDB getTablesInDB() {
        DefaultDescriptor desc = (DefaultDescriptor) this.getDescriptor();
        final TableInDB tableInDB = TableInDB.create(new DBIdentity() {
            @Override
            public boolean isEquals(DBIdentity queryDBSourceId) {
                return true;
            }

            @Override
            public String identityValue() {
                return desc.getEndType().getVal();
            }
        });
        return tableInDB;
    }

    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {
        IDataxReaderContext readerContext = new DataXDFSReaderContext(this);
        return IGroupChildTaskIterator.create(readerContext);
    }


    @FormField(ordinal = 16, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDFSReader.class, "DataXDFSReader-tpl.json");
    }

    @Override
    public boolean hasMulitTable() {
        return false;
    }

    @Override
    public List<ISelectedTab> getSelectedTabs() {

        return this.dfsLinker.useTdfsSession((session) -> {

            Set<ITDFSSession.Res> matchRes = resMatcher.findAllRes(session);
            for (ITDFSSession.Res res : matchRes) {
                try (InputStream resStream = session.getInputStream(res.fullPath)) {

                    FileFormat.FileHeader fileHeader = this.fileFormat.readHeader(resStream);

                    ParseColsResult parseColsResult = ParseColsResult.parseColsResult(DataXDFSReaderContext.FTP_TASK, fileHeader);
                    if (!parseColsResult.success) {
                        throw new IllegalStateException("parseColsResult must be success");
                    }
                    return Collections.singletonList(parseColsResult.tabMeta);
                }
            }
            throw new IllegalStateException("have not find any matchRes by resMatcher:" + this.resMatcher);
        });


//        DefaultContext context = new DefaultContext();
//        ParseColsResult parseColsResult = ParseColsResult.parseColsCfg(DataXFtpReaderContext.FTP_TASK,
//                new DefaultFieldErrorHandler(), context, StringUtils.EMPTY, this.column);
//        if (!parseColsResult.success) {
//            throw new IllegalStateException("parseColsResult must be success");
//        }
//        return Collections.singletonList(parseColsResult.tabMeta);
    }
//
//    private Set<String> getAllMatchedRes(ITDFSSession session) {
//        return session.findAllRes(this.resMatcher);
//    }


    @Override
    public String getTemplate() {
        return template;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
            registerSelectOptions(FtpTDFSLinker.KEY_FTP_SERVER_LINK, () -> ParamsConfig.getItems(FTPServer.FTP_SERVER));
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
            DataXDFSReader dataxReader = (DataXDFSReader) postFormVals.newInstance(this, msgHandler);

            Set<ITDFSSession.Res> matchRes
                    = dataxReader.dfsLinker.useTdfsSession((session) -> {
                return dataxReader.resMatcher.findAllRes(session);
            });

            if (CollectionUtils.isEmpty(matchRes)) {
                msgHandler.addFieldError(context, KEY_RES_MATCHER
                        , "路径:" + dataxReader.dfsLinker.getRootPath() + "下，使用匹配:'" + dataxReader.resMatcher + "'不能找到对应的资源文件");
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
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return this.getEndType().name();
        }
    }
}
