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
import com.alibaba.datax.plugin.ftp.common.FtpHelper;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.SubFormFilter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.datax.meta.DefaultMetaDataWriter;
import com.qlangtech.tis.plugin.datax.resmatcher.WildcardDFSResMatcher;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 导入FTP上有符合TIS要求的Metadata元数据文件的 FTP文件
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.ftpreader.FtpReader
 * @see com.qlangtech.tis.plugin.datax.meta.DefaultMetaDataWriter
 **/
@Public
public class DataXDFSReaderWithMeta extends AbstractDFSReader {


//    @SubForm(desClazz = SelectedTab.class
//            , idListGetScript = "return com.qlangtech.tis.plugin.datax.DataXDFSReaderWithMeta.getDFSFiles(filter);", atLeastOne = true)
//    public transient List<SelectedTab> selectedTabs;

    public DataXDFSReaderWithMeta() {
        WildcardDFSResMatcher matcher = new WildcardDFSResMatcher();
        matcher.maxTraversalLevel = 2;
        matcher.wildcard = "*" + FtpHelper.KEY_META_FILE;
        this.resMatcher = matcher;
    }

    @Override
    public List<TargetResMeta> getSelectedEntities() {
        throw new UnsupportedOperationException();
    }

    public static List<ColumnMetaData> getDFSFileMetaData(String metaParentPath, ITDFSSession dfs) {
        final String dfsPath = IPath.pathConcat(metaParentPath, FtpHelper.KEY_META_FILE);
        return getMetaData(dfsPath, dfs);
    }


    private static List<ColumnMetaData> getMetaData(final String dfsPath, ITDFSSession dfs) {
        String content = null;
        try (InputStream reader = Objects.requireNonNull(dfs.getInputStream(dfsPath)
                , "path:" + dfsPath + " relevant InputStream can not null")) {
            content = IOUtils.toString(reader, TisUTF8.get());

            JSONArray fields = JSONArray.parseArray(content);
            return DefaultMetaDataWriter.deserialize(fields);
        } catch (Exception e) {
            throw new RuntimeException("dfsPath:" + dfsPath, e);
        }
    }

    private List<ColumnMetaData> getFTPFileMetaData(EntityName table, ITDFSSession dfs) {

        final String ftpPath = IPath.pathConcat(this.dfsLinker.getRootPath(),
                table.getTabName(), FtpHelper.KEY_META_FILE);
        return getMetaData(ftpPath, dfs);

//        try (InputStream reader = Objects.requireNonNull(ftp.getInputStream(ftpPath), "path:" + ftpPath + " relevant InputStream can not null")) {
//            content = IOUtils.toString(reader, TisUTF8.get());
//
//            JSONArray fields = JSONArray.parseArray(content);
//            return DefaultMetaDataWriter.deserialize(fields);
//        } catch (Exception e) {
//            throw new RuntimeException(content, e);
//        }
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, IPluginContext context, EntityName table) throws TableNotFoundException {
        return this.dfsLinker.useTdfsSession((dfs) -> {
            return getFTPFileMetaData(table, dfs);
        });
//        FTPServer server = FTPServer.getServer(this.linker);
//        return server.useFtpHelper((ftp) -> {
//            return getFTPFileMetaData(table, ftp);
//        });
    }


    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {

        final List<ISelectedTab> tabs = selectedTabs;
        final int tabsLength = tabs.size();
        AtomicInteger selectedTabIndex = new AtomicInteger(0);
        ConcurrentHashMap<String, List<DBDataXChildTask>> groupedInfo = new ConcurrentHashMap();

//        FTPServer server = FTPServer.getServer(this.linker);

        ITDFSSession dfs = this.dfsLinker.createTdfsSession();
        //  final FtpHelper dfs = server.createFtpHelper(server.timeout);
        return new IGroupChildTaskIterator() {
            int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return ((currentIndex = selectedTabIndex.getAndIncrement()) < tabsLength);
            }

            @Override
            public IDataxReaderContext next() {
                ISelectedTab tab = tabs.get(currentIndex);

                ColumnMetaData.fillSelectedTabMeta(tab, (t) -> {
                    List<ColumnMetaData> colsMeta = getFTPFileMetaData(EntityName.parse(t.getName()), dfs);
                    return colsMeta.stream().collect(Collectors.toMap((c) -> c.getKey(), (c) -> c));
                });

                List<DBDataXChildTask> childTasks
                        = groupedInfo.computeIfAbsent(tab.getName(), (tabname) -> Lists.newArrayList());
                String childTask = tab.getName() + "_" + currentIndex;

                childTasks.add(new DBDataXChildTask(StringUtils.EMPTY
                        , DataXDFSReaderContext.FTP_TASK, childTask));

                return new DataXDFSSelectTableReaderContext(DataXDFSReaderWithMeta.this, tab, currentIndex);
            }

            @Override
            public Map<String, List<DBDataXChildTask>> getGroupedInfo() {
                return groupedInfo;
            }

            @Override
            public void close() throws IOException {
                try {
                    dfs.close();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        };
    }


    @Override
    public boolean hasMulitTable() {
        return CollectionUtils.isNotEmpty(this.selectedTabs);// getSelectedTabs().size() > 0;;
    }


    @Override
    public List<ISelectedTab> getSelectedTabs() {
        return this.selectedTabs.stream().collect(Collectors.toList());
    }

    @Override
    public FileFormat getFileFormat(Optional<String> entityName) {
        throw new UnsupportedOperationException();
    }

    public static final Pattern FTP_FILE_PATTERN
            = Pattern.compile(".+?([^/]+)" + IOUtils.DIR_SEPARATOR + StringUtils.replace(FtpHelper.KEY_META_FILE, ".", "\\."));

    public static List<String> getDFSFiles(SubFormFilter filter) {
        AbstractDFSReader reader = DataxReader.getDataxReader(filter);
        return getFTPFiles(reader).stream().map((meta) -> meta.entityName).collect(Collectors.toList());
    }

    public static TargetResMeta getTargetResMeta(ITDFSSession.Res meta) {
        return getTargetResMeta(meta.fullPath);
    }

    public static TargetResMeta getTargetResMeta(String fullMetapath) {
        Matcher matcher = FTP_FILE_PATTERN.matcher(fullMetapath);
        String resEntityName = null;
        if (matcher.matches()) {
            resEntityName = (matcher.group(1));
        } else {
            // throw new IllegalStateException("target res:" + fullMetapath + " is not match Pattern:" + FTP_FILE_PATTERN);
            return null;
        }
        return new TargetResMeta(resEntityName, (dfs) -> {
            return getMetaData(fullMetapath, dfs);
        });
    }

    public static class TargetResMeta {
        private final String entityName;
        private Function<ITDFSSession, List<ColumnMetaData>> colsMetaCreator;

        public TargetResMeta(String entityName, Function<ITDFSSession, List<ColumnMetaData>> colsMetaCreator) {
            this.entityName = entityName;
            this.colsMetaCreator = colsMetaCreator;
        }

        public String getEntityName() {
            return entityName;
        }

        public List<ColumnMetaData> getColsMeta(ITDFSSession dfs) {
            return colsMetaCreator.apply(dfs);
        }
    }


    public static List<TargetResMeta> getFTPFiles(AbstractDFSReader reader) {

        return reader.getSelectedEntities();


    }


    // @TISExtension()
    public static class DefaultDescriptor extends DataXDFSReader.DefaultDescriptor {
        public DefaultDescriptor() {
            super();
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return true;
        }

        public boolean validateCsvReaderConfig(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return PluginFieldValidators.validateCsvReaderConfig(msgHandler, context, fieldName, value);
        }

        @Override
        public boolean isRdbms() {
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, Descriptor.PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, Descriptor.PostFormVals postFormVals) {
            DataXDFSReaderWithMeta dataxReader = (DataXDFSReaderWithMeta) postFormVals.newInstance();

            List<TargetResMeta> ftpFiles = DataXDFSReaderWithMeta.getFTPFiles(dataxReader);
            if (CollectionUtils.isEmpty(ftpFiles)) {
                msgHandler.addFieldError(context, KEY_DFS_LINKER, "该路径下未扫描到" + super.getDisplayName() + "元数据文件:" + FtpHelper.KEY_META_FILE);
                return false;
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return super.getDisplayName() + "-Meta";
        }
    }
}
