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

package com.alibaba.datax.plugin.writer.ftpwriter;

import com.alibaba.datax.core.job.IJobContainerContext;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.DataXDFSWriter;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;

import java.io.OutputStream;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-05 15:38
 **/
public class TISFtpWriter extends FtpWriter {

    public static class Job extends FtpWriter.Job {
        private DataXDFSWriter writer;

        @Override
        public void init() {
            this.writer = getDfsWriter(this.containerContext);
            super.init();
        }


        @Override
        protected ITDFSSession createTdfsSession() {
            return this.writer.dfsLinker.createTdfsSession();
        }

        @Override
        protected String createTargetPath(String tableName) {
            return getFtpTargetDir(tableName, this.writer);
        }
    }

    private static DataXDFSWriter getDfsWriter(IJobContainerContext containerContext) {
        return Objects.requireNonNull((DataXDFSWriter) DataxWriter.load(null, containerContext.getTISDataXName())
                , "writer can not be null");
    }

    private static String getFtpTargetDir(String tableName, DataXDFSWriter writer) {
        // IDataxProcessor processor = DataxProcessor.load(null, containerContext.getTISDataXName());
        // DataXFtpWriter writer = (DataXFtpWriter) DataxWriter.load(null, containerContext.getTISDataXName());
        //  DataXFtpWriter writer = (DataXFtpWriter) processor.getWriter(null);
        return writer.writeMetaData.getDfsTargetDir(writer.dfsLinker, tableName);
    }

    public static class Task extends FtpWriter.Task {
        private DataXDFSWriter writer;

        @Override
        public void init() {
            this.writer = getDfsWriter(this.containerContext);
            super.init();
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            return Objects.requireNonNull(this.writer, "writer can not be null").dfsLinker.createTdfsSession();
        }

        @Override
        protected UnstructuredWriter createUnstructuredWriter(OutputStream w) {
            return writer.fileFormat.createWriter(w);
        }
    }
}
