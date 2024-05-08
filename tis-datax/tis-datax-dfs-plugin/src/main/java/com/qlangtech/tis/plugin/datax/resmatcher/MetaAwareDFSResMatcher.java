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

package com.qlangtech.tis.plugin.datax.resmatcher;

import com.alibaba.datax.plugin.ftp.common.FtpHelper;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.plugin.datax.DataXDFSReaderContext;
import com.qlangtech.tis.plugin.datax.DataXDFSReaderWithMeta;
import com.qlangtech.tis.plugin.datax.DataXDFSSelectTableReaderContext;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 利用dfs writer时候向dfs中写入的metadata元数据文件，在运行期读取该metadata file 获取dfs文件对应的元数据信息，从而可大大简化dfs文件的导入流程设置
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-13 22:25
 **/
public class MetaAwareDFSResMatcher extends BasicDFSResMatcher {
    public MetaAwareDFSResMatcher() {
        this.maxTraversalLevel = 2;
    }

    @Override
    public List<ISelectedTab> getSelectedTabs(IDFSReader dfsReader) {
        // 避免出现栈溢出
        return Objects.requireNonNull(((Supplier<List<SelectedTab>>) dfsReader).get(), "selectedTabs can not be null")
                .stream().collect(Collectors.toList());
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(IDFSReader dfsReader, EntityName table) throws TableNotFoundException {
        return dfsReader.getDfsLinker().useTdfsSession((dfs) -> {
            return getDFSFileMetaData(dfsReader, table, dfs);
        });
    }


    @Override
    public SourceColsMeta getSourceColsMeta(ITDFSSession hdfsSession, Optional<String> entityName, String path, IDataxProcessor processor) {

        // IDataxReader reader = processor.getReader(null);
        // List<ISelectedTab> selectedTabs = reader.getSelectedTabs();
        String fullMetaPath = IPath.pathConcat(path, FtpHelper.KEY_META_FILE);
       // String tabName = entityName.orElseThrow(() -> new IllegalStateException(" entitName must present"));
        DataXDFSReaderWithMeta.TargetResMeta resMeta = DataXDFSReaderWithMeta.getTargetResMeta(fullMetaPath);
        if (resMeta == null) {
            throw new IllegalStateException("resMeta can not be null ,fullMetaPath:" + fullMetaPath);
        }

        return getSourceColsMeta(processor, resMeta.getEntityName(), DataXDFSReaderWithMeta.getDFSFileMetaData(path, hdfsSession));

//        Optional<ISelectedTab> tab = selectedTabs.stream().filter((t) -> StringUtils.equals(resMeta.getEntityName(), t.getName())).findFirst();
//
//        ISelectedTab ttab = tab.orElseThrow(() -> new IllegalStateException("can not find tab:" + resMeta.getEntityName() + " in select tables:"
//                + selectedTabs.stream().map((t) -> t.getName()).collect(Collectors.joining(","))));
//
//        final Set<String> selectedCols
//                = ttab.getCols().stream().map((c) -> c.getName()).collect(Collectors.toSet());
//
//
//        List<CMeta> result = Lists.newArrayList();
//        CMeta cm = null;
//        List<ColumnMetaData> colsMeta = DataXDFSReaderWithMeta.getDFSFileMetaData(path, hdfsSession);
//        for (ColumnMetaData col : colsMeta) {
//            cm = new CMeta();
//            cm.setPk(col.isPk());
//            cm.setType(col.getType());
//            cm.setName(col.getName());
//            cm.setNullable(col.isNullable());
//            result.add(cm);
//        }
//        return new SourceColsMeta(result, (col) -> selectedCols.contains(col));
    }




    @Override
    public boolean hasMulitTable(IDFSReader dfsReader) {
        return CollectionUtils.isNotEmpty(dfsReader.getSelectedTabs());
    }

    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter, IDFSReader dfsReader) {
        final List<ISelectedTab> tabs = dfsReader.getSelectedTabs();
        final int tabsLength = tabs.size();
        AtomicInteger selectedTabIndex = new AtomicInteger(0);
        ConcurrentHashMap<String, List<DataXCfgGenerator.DBDataXChildTask>> groupedInfo = new ConcurrentHashMap();

//        FTPServer server = FTPServer.getServer(this.linker);

        ITDFSSession dfs = dfsReader.getDfsLinker().createTdfsSession();
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
                    List<ColumnMetaData> colsMeta = getDFSFileMetaData(dfsReader, EntityName.parse(t.getName()), dfs);
                    return colsMeta.stream().collect(Collectors.toMap((c) -> c.getKey(), (c) -> c));
                });

                List<DataXCfgGenerator.DBDataXChildTask> childTasks
                        = groupedInfo.computeIfAbsent(tab.getName(), (tabname) -> Lists.newArrayList());
                String childTask = tab.getName() + "_" + currentIndex;

                childTasks.add(new DataXCfgGenerator.DBDataXChildTask(StringUtils.EMPTY
                        , DataXDFSReaderContext.FTP_TASK, childTask));

                return new DataXDFSSelectTableReaderContext(dfsReader, tab, currentIndex);
            }

            @Override
            public Map<String, List<DataXCfgGenerator.DBDataXChildTask>> getGroupedInfo() {
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

    /**
     * @param dfsReader
     * @param table
     * @param dfs
     * @return
     * @see TDFSLinker
     */
    private List<ColumnMetaData> getDFSFileMetaData(IDFSReader dfsReader, EntityName table, ITDFSSession dfs) {

//                IPath.pathConcat(dfsReader.getDfsLinker().getRootPath(), table.getTabName());
//
//        String[] pathSplit = StringUtils.split(dfsReader.getDfsLinker().getRootPath(), File.separator);
//        if (pathSplit.length > 0) {
//            /**
//             * TDFSLinker 的path 设置 为 meta.json 的直接父目录的话，则不需要再在path 添加表名称了，不然路径不对了
//             */
//            if (pathSplit[pathSplit.length - 1].equals(table.getTabName())) {
//                ftpPath = dfsReader.getDfsLinker().getRootPath();
//            }
//        }
        return DataXDFSReaderWithMeta.getDFSFileMetaData(
                DataXDFSSelectTableReaderContext.buildMetaAwareDFSTargetPath(
                        dfsReader.getDfsLinker(), table.getTabName()), dfs);
    }


    /**
     * 匹配数据资源文件
     *
     * @param testRes
     * @return
     */
    @Override
    public boolean isMatch(ITDFSSession.Res testRes) {
        return !FilenameUtils.wildcardMatch(testRes.relevantPath, "*" + FtpHelper.KEY_META_FILE);
    }

    @Override
    public boolean isRDBMSSupport() {
        return true;
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<DFSResMatcher> {
        @Override
        public String getDisplayName() {
            return "ByMeta";
        }
    }
}
