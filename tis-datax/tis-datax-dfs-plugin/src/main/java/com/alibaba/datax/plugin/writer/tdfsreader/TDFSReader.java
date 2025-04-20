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

package com.alibaba.datax.plugin.writer.tdfsreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.core.job.IJobContainerContext;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReader;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.datax.AbstractDFSReader;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 11:11
 **/
public class TDFSReader extends FtpReader {

    public static class Job extends FtpReader.Job {
        private AbstractDFSReader reader;

        protected TDFSLinker getDFSLinker() {
            return Objects.requireNonNull(reader.dfsLinker, "reader.dfsLinker can not be null");
        }

        protected <T extends AbstractDFSReader> T getReader() {
            return (T) this.reader;
        }

        @Override
        public void init() {
            this.reader = getDfsReader(this.containerContext);
            super.init();
        }

        @Override
        protected Set<ITDFSSession.Res> findAllMatchedRes(List<String> targetPath, ITDFSSession dfsSession) {
            if (CollectionUtils.isEmpty(targetPath)) {
                throw new IllegalArgumentException("param targetPath can not be null");
            }
            return reader.resMatcher.findAllRes(dfsSession, targetPath);
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            return this.getDFSLinker().createTdfsSession();
        }
    }

    public static class Task extends FtpReader.Task {
        private AbstractDFSReader reader;
        private List<ColumnEntry> cols;
        private FileFormat fileFormat;

        public <T extends FileFormat> T getFileFormat() {
            return (T) fileFormat;
        }

        @Override
        public void init() {
            this.reader = getDfsReader(this.containerContext);
            super.init();
        }

        protected TDFSLinker getDFSLinker() {
            return Objects.requireNonNull(reader.dfsLinker, "reader.dfsLinker can not be null");
        }

        /**
         * @param paths     value of Key.PATH
         * @param processor
         * @return
         */
        private DFSResMatcher.SourceColsMeta getSourceColsMeta(ITDFSSession hdfsSession
                , Optional<String> entityName, List<String> paths, IDataxProcessor processor) {
            if (CollectionUtils.isEmpty(paths)) {
                throw new IllegalArgumentException("param path can not be empty");
            }
            if (hdfsSession == null) {
                throw new IllegalArgumentException("param hdfsSession can not be null");
            }
            for (String path : paths) {
                return this.reader.resMatcher.getSourceColsMeta(hdfsSession, entityName, path, processor);
            }
            throw new IllegalStateException(" can not find relevant instance of SourceColsMeta");
        }

        @Override
        protected List<ColumnEntry> createColsMeta(Optional<String> entityName) {

            if (this.cols == null) {
                IDataxProcessor processor = DataxProcessor.load(null, containerContext.getTISDataXName());

                DFSResMatcher.SourceColsMeta sourceCols = this.getSourceColsMeta(this.hdfsSession, entityName, getPaths(this.getPluginJobConf()), processor);

                this.cols = Lists.newArrayList();
                ColumnEntry ce = null;
                this.fileFormat = reader.getFileFormat(entityName);
                Function<String, Column> colValCreator = null;
                int index = 0;
                for (CMeta c : sourceCols.getColsMeta()) {
                    try {
                        if (!sourceCols.colSelected(c.getName())) {
                            // source 表选择了部分列的情况下
                            continue;
                        }

                        ce = new ColumnEntry(c instanceof CMeta.VirtualCMeta);
                        ce.setIndex(index);
                        colValCreator = this.fileFormat.buildColValCreator(c);
                        ce.setColName(c.getName());
                        ce.setType(c.getType().getJdbcType().getLiteria(), colValCreator);
                        cols.add(ce);
                    } finally {
                        index++;
                    }
                }
                this.cols = Collections.unmodifiableList(this.cols);
                this.unstructuredReaderCreator = (reader) -> {
                    return Objects.requireNonNull(this.fileFormat, "fileFormat can not be null").createReader(reader, sourceCols.getColsMeta());
                };
            }

            return this.cols;
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            //  this.nullFormat = this.reader.fileFormat


            return reader.dfsLinker.createTdfsSession();
        }
    }


    private static AbstractDFSReader getDfsReader(IJobContainerContext containerContext) {
        return Objects.requireNonNull((AbstractDFSReader)
                DataxReader.load(null, containerContext.getTISDataXName().getPipelineName()), "writer can not be null");
    }
}
