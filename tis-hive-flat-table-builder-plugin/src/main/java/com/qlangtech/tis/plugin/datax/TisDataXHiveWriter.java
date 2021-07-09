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
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.offline.DataxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 16:55
 **/
public class TisDataXHiveWriter extends Writer {

    public static final String KEY_HIVE_TAB_NAME = "hiveTableName";


    static final Logger logger = LoggerFactory.getLogger(TisDataXHiveWriter.class);
    static final Field jobColumnsField;
    static final Field jobFileType;
    static final Field jobFieldDelimiter;
    static final Field jobPath;

    static {
        jobColumnsField = getJobField("columns");
        jobFileType = getJobField("fileType");
        jobFieldDelimiter = getJobField("fieldDelimiter");
        jobPath = getJobField("path");
    }

    private static Field getJobField(String fieldName) {
        try {
            Field field = HdfsWriter.Job.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Job extends BasicEngineJob<DataXHiveWriter> {

    }

    public static class Task extends HdfsWriter.Task {
        private Configuration cfg;

        @Override
        public void init() {
            try {
                super.init();
            } catch (Throwable e) {
                logger.warn("init alibaba hdfs writer task faild,errmsg:" + e.getMessage());
            }
            validateFileNameVal();
            BasicHdfsWriterJob.setHdfsHelper(this.getPluginJobConf(), BasicHdfsWriterJob.taskHdfsHelperField, getHdfsWriterPlugin(getPluginJobConf()), this);
        }

        private void validateFileNameVal() {
            Object val = null;
            try {
                Field fileName = HdfsWriter.Task.class.getDeclaredField("fileName");
                fileName.setAccessible(true);
                val = fileName.get(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (val == null) {
                throw new IllegalStateException("fileName prop have not been init valid");
            }
        }

    }


    static <TT extends BasicFSWriter> TT getHdfsWriterPlugin(Configuration cfg) {
        String dataxName = cfg.getString(DataxUtils.DATAX_NAME);
        DataxWriter dataxWriter = DataxWriter.load(null, dataxName);
        if (!(dataxWriter instanceof BasicFSWriter)) {
            throw new BasicEngineJob.JobPropInitializeException("datax Writer must be type of 'BasicFSWriter',but now is:" + dataxWriter.getClass());
        }
        return (TT) dataxWriter;
    }

}
