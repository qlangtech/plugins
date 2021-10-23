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

import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.alibaba.datax.plugin.writer.hdfswriter.Key;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 16:55
 **/
public class TisDataXHdfsWriter extends Writer {

    // public static final String KEY_HIVE_TAB_NAME = "hiveTableName";

    // private static final Logger logger = LoggerFactory.getLogger(TisDataXHdfsWriter.class);

    public static class Job extends BasicHdfsWriterJob<DataXHdfsWriter> {

//        protected int getPtRetainNum() {
//            return -1;
//        }

//        @Override
//        protected EntityName createDumpTable() {
//            return null;
//        }
//
//        @Override
//        protected void initializeHiveTable(List<HiveColumn> cols) {
//            // super.initializeHiveTable(cols);
//        }
//
//        @Override
//        protected void bindHiveTables() {
//            // super.bindHiveTables();
//        }

        //        protected Path getPmodPath() {
//            return this.tabDumpParentPath;
//        }
//

        @Override
        protected Path createPath() throws IOException {
            ITISFileSystem fs = this.getWriterPlugin().getFs().getFileSystem();
            this.tabDumpParentPath = new Path(fs.getRootDir()
                    , this.cfg.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE));
            HdfsPath p = new HdfsPath(this.tabDumpParentPath);
            if (!fs.exists(p)) {
                fs.mkdirs(p);
            }
            return this.tabDumpParentPath;
        }
    }


    public static class Task extends TisDataXHiveWriter.Task {

//        @Override
//        public void init() {
//            try {
//                super.init();
//            } catch (Throwable e) {
//                logger.warn("init alibaba hdfs writer task faild,errmsg:" + e.getMessage());
//            }
//            validateFileNameVal();
//            setHdfsHelper(this.getPluginJobConf(), this);
//        }
//
//        private void validateFileNameVal() {
//            Object val = null;
//            try {
//                Field fileName = HdfsWriter.Task.class.getDeclaredField("fileName");
//                fileName.setAccessible(true);
//                val = fileName.get(this);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            if (val == null) {
//                throw new IllegalStateException("fileName prop have not been init valid");
//            }
//        }

    }

}