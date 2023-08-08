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

import com.alibaba.datax.core.job.IJobContainerContext;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReader;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.datax.DataXFtpReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 11:11
 **/
public class TDFSReader extends FtpReader {

    public static class Job extends FtpReader.Job {
        private DataXFtpReader reader;

        @Override
        public void init() {
            this.reader = getDfsReader(this.containerContext);
            super.init();
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            return reader.dfsLinker.createTdfsSession();
        }
    }

    public static class Task extends FtpReader.Task {
        private DataXFtpReader reader;

        @Override
        public void init() {
            this.reader = getDfsReader(this.containerContext);
            super.init();
        }

        @Override
        protected ITDFSSession createTdfsSession() {
            return reader.dfsLinker.createTdfsSession();
        }
    }


    private static DataXFtpReader getDfsReader(IJobContainerContext containerContext) {
        return Objects.requireNonNull((DataXFtpReader) DataxReader.load(null, containerContext.getTISDataXName())
                , "writer can not be null");
    }
}
