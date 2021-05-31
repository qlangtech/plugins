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
import com.qlangtech.tis.hive.HiveColumn;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 16:55
 **/
public class TisDataXHdfsWriter extends Writer {

    // public static final String KEY_HIVE_TAB_NAME = "hiveTableName";

    // private static final Logger logger = LoggerFactory.getLogger(TisDataXHdfsWriter.class);

    public static class Job extends TisDataXHiveWriter.Job {
//        @Override
//        protected DataXHdfsWriter getWriterPlugin() {
//            return super.getWriterPlugin();
//        }
//        @Override
//        public void init() {
//            super.init();
//        }
//
//        public void prepare() {
//            super.prepare();
//        }
//
//        @Override
//        public void post() {
//            super.post();
//        }

        @Override
        protected EntityName createDumpTable() {
            return null;
        }

        @Override
        protected void initializeHiveTable(List<HiveColumn> cols) {
            // super.initializeHiveTable(cols);
        }

        @Override
        protected void bindHiveTables() {
            // super.bindHiveTables();
        }

        protected Path getPmodPath() {
            return this.tabDumpParentPath;
        }

        protected String getHdfsSubPath() {
            return this.cfg.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
//            String jobPathProp = null;
//            try {
//                ;
//                jobPathProp = this.cfg.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
//            } catch (IllegalAccessException e) {
//                throw new RuntimeException(e);
//            }
//            if (StringUtils.isBlank(jobPathProp)) {
//                throw new IllegalStateException("prop job path can not be null");
//            }
//            return jobPathProp;
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
