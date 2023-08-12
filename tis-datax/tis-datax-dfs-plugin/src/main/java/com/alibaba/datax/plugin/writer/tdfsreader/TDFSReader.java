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
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.datax.DataXDFSReader;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;

import java.util.*;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 11:11
 **/
public class TDFSReader extends FtpReader {

    public static class Job extends FtpReader.Job {
        private DataXDFSReader reader;

        @Override
        public void init() {
            this.reader = getDfsReader(this.containerContext);
            super.init();
        }

        @Override
        protected Set<ITDFSSession.Res> findAllMatchedRes(ITDFSSession dfsSession) {
            return reader.resMatcher.findAllRes(dfsSession);
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            return reader.dfsLinker.createTdfsSession();
        }
    }

    public static class Task extends FtpReader.Task {
        private DataXDFSReader reader;
        private List<ColumnEntry> cols;

        @Override
        public void init() {
            this.reader = getDfsReader(this.containerContext);
            super.init();
        }

        @Override
        protected List<ColumnEntry> createColsMeta() {

            if (this.cols == null) {
                IDataxProcessor processor = DataxProcessor.load(null, containerContext.getTISDataXName());
                TableAliasMapper tabAlias = processor.getTabAlias(null);
                Optional<TableAlias> findMapper = tabAlias.findFirst();
                IDataxProcessor.TableMap tabMapper
                        = (IDataxProcessor.TableMap) findMapper.orElseThrow(() -> new NullPointerException("TableAlias can not be null"));
                // BasicPainFormat fileFormat = (BasicPainFormat) reader.fileFormat;

                this.cols = Lists.newArrayList();
                ColumnEntry ce = null;
                List<CMeta> sourceCols = tabMapper.getSourceCols();
                Function<String, Column> colValCreator = null;
                int index = 0;
                for (CMeta c : sourceCols) {
                    ce = new ColumnEntry();
                    ce.setIndex(index++);
                    colValCreator = reader.fileFormat.buildColValCreator(c);
                    ce.setColName(c.getName());
                    ce.setType(c.getType().typeName, colValCreator);
                    cols.add(ce);
                }
                this.cols = Collections.unmodifiableList(this.cols);
            }

            return this.cols;
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            //  this.nullFormat = this.reader.fileFormat

            this.unstructuredReaderCreator = (reader) -> {
                return this.reader.fileFormat.createReader(reader);
            };
            return reader.dfsLinker.createTdfsSession();
        }
    }


    private static DataXDFSReader getDfsReader(IJobContainerContext containerContext) {
        return Objects.requireNonNull((DataXDFSReader) DataxReader.load(null, containerContext.getTISDataXName())
                , "writer can not be null");
    }
}
