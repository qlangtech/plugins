/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hdfsreader.AbstractDFSUtil;
import com.alibaba.datax.plugin.reader.hdfsreader.HdfsReader;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.offline.DataxUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-11 19:13
 **/
public class TisDataXHdfsReader extends HdfsReader {

    public static class Job extends HdfsReader.Job {
        @Override
        protected AbstractDFSUtil createDfsUtil() {
            return new TISDFSUtil(this.readerOriginConfig);
        }
    }

    public static class Task extends HdfsReader.Task {
        @Override
        protected AbstractDFSUtil createDfsUtil() {
            return new TISDFSUtil(this.taskConfig);
        }
    }

    private static class TISDFSUtil extends AbstractDFSUtil {
        private DataXHdfsReader dataXReader;

        public TISDFSUtil(com.alibaba.datax.common.util.Configuration cfg) {
            super(new org.apache.hadoop.conf.Configuration());
            this.dataXReader = getHdfsReaderPlugin(cfg);
        }

        private DataXHdfsReader getHdfsReader() {
            return this.dataXReader;
        }

        @Override
        protected FileSystem getFileSystem() throws IOException {
            return getHdfsReader().getFs().getFileSystem().unwrap();
        }
    }

    private static DataXHdfsReader getHdfsReaderPlugin(Configuration cfg) {
        String dataxName = cfg.getNecessaryValue(DataxUtils.DATAX_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
        DataxReader dataxReader = DataxReader.load(null, dataxName);
        if (!(dataxReader instanceof DataXHdfsReader)) {
            throw new BasicEngineJob.JobPropInitializeException(
                    "datax reader must be type of 'DataXHdfsReader',but now is:" + dataxReader.getClass());
        }
        return (DataXHdfsReader) dataxReader;
    }
}
