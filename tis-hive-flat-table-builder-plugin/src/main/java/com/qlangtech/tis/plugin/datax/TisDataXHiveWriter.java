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
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.alibaba.datax.plugin.writer.hdfswriter.Key;
import com.alibaba.datax.plugin.writer.hdfswriter.SupportHiveDataType;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.dump.hive.BindHiveTableTool;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.taskflow.hive.JoinHiveTask;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.hive.HdfsFileType;
import com.qlangtech.tis.hive.HdfsFormat;
import com.qlangtech.tis.hive.HiveColumn;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 16:55
 **/
public class TisDataXHiveWriter extends Writer {

    public static final String KEY_HIVE_TAB_NAME = "hiveTableName";

    static final Field jobHdfsHelperField;
    static final Field taskHdfsHelperField;

    static {
        try {
            jobHdfsHelperField = HdfsWriter.Job.class.getDeclaredField("hdfsHelper");
            jobHdfsHelperField.setAccessible(true);

            taskHdfsHelperField = HdfsWriter.Task.class.getDeclaredField("hdfsHelper");
            taskHdfsHelperField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TisDataXHiveWriter.class);
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

    public static class Job extends HdfsWriter.Job {
        private ITISFileSystem fileSystem = null;
        protected Configuration cfg = null;
        private DataXHdfsWriter writerPlugin = null;
        private EntityName dumpTable = null;
        private List<HiveColumn> colsExcludePartitionCols = null;
        private String dumpTimeStamp;
        protected Path tabDumpParentPath;
        private Integer ptRetainNum;

        @Override
        public void init() {
            try {
                super.init();
            } catch (Throwable e) {
                if (ExceptionUtils.indexOfType(e, JobPropInitializeException.class) > -1) {
                    throw new RuntimeException(e);
                } else {
                    logger.warn("init alibaba hdfs writer Job faild,errmsg:" + StringUtils.substringBefore(e.getMessage(), "\n"));
                }
            }

            this.ptRetainNum = Integer.parseInt(this.cfg.getNecessaryValue("ptRetainNum", HdfsWriterErrorCode.REQUIRED_VALUE));

            Objects.requireNonNull(writerPlugin, "writerPlugin can not be null");
            //this.getDumpTable();

            Objects.requireNonNull(this.dumpTimeStamp, "dumpTimeStamp can not be null");

            // try {
//                this.tabDumpParentPath = new Path(this.writerPlugin.getFs().getFileSystem().getRootDir(), getHdfsSubPath());
//                Path pmodPath = getPmodPath();
//                // 将path创建
//                this.writerPlugin.getFs().getFileSystem().mkdirs(new HdfsPath(pmodPath));
//                jobPath.set(this, pmodPath.toString());
//                logger.info("hive writer hdfs path:{}", pmodPath);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }


            setHdfsHelper(this.getPluginJobConf(), jobHdfsHelperField, this.writerPlugin, this);
        }

        public void prepare() {
            super.prepare();

            // 需要自动创建hive表
            this.fileSystem = writerPlugin.getFs().getFileSystem();
            Objects.requireNonNull(fileSystem, "fileSystem can not be null");

            this.colsExcludePartitionCols = getCols();
            int[] appendStartIndex = new int[]{colsExcludePartitionCols.size()};
            List<HiveColumn> cols = Lists.newArrayList(colsExcludePartitionCols);

            IDumpTable.preservedPsCols.forEach((c) -> {
                HiveColumn hiveCol = new HiveColumn();
                hiveCol.setName(c);
                hiveCol.setType(SupportHiveDataType.STRING.name());
                hiveCol.setIndex(appendStartIndex[0]++);
                cols.add(hiveCol);
            });
            initializeHiveTable(cols);
        }

        @Override
        public Configuration getPluginJobConf() {
            this.cfg = super.getPluginJobConf();
            this.writerPlugin = getHdfsWriterPlugin(this.cfg);
            try {
                if (this.tabDumpParentPath == null) {
                    SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                    this.dumpTimeStamp = timeFormat.format(new Date());
                    this.dumpTable = this.createDumpTable();
                    this.tabDumpParentPath = new Path(this.writerPlugin.getFs().getFileSystem().getRootDir(), getHdfsSubPath());
                    Path pmodPath = getPmodPath();
                    // 将path创建
                    HdfsFileSystemFactory hdfsFactory = (HdfsFileSystemFactory) this.writerPlugin.getFs();
                    hdfsFactory.getFileSystem().mkdirs(new HdfsPath(pmodPath));
                    //jobPath.set(this, pmodPath.toString());
                    cfg.set(Key.PATH, pmodPath.toString());
                    logger.info("config param {}:{}", Key.PATH, hdfsFactory.hdfsAddress);
                    cfg.set(Key.DEFAULT_FS, hdfsFactory.hdfsAddress);
                }
            } catch (Throwable e) {
                throw new JobPropInitializeException("pmodPath initial error", e);
            }

            // 写了一个默认的可以骗过父类校验
            return cfg;
        }

//        protected DataXHdfsWriter getWriterPlugin() {
//            return getHdfsWriterPlugin(this.cfg);
//        }

        private static class JobPropInitializeException extends RuntimeException {
            public JobPropInitializeException(String message, Throwable cause) {
                super(message, cause);
            }
        }


        protected Path getPmodPath() {
            return new Path(tabDumpParentPath, "0");
        }

        protected String getHdfsSubPath() {
            Objects.requireNonNull(dumpTable, "dumpTable can not be null");
            return this.dumpTable.getNameWithPath() + "/" + this.dumpTimeStamp;
        }

        protected EntityName createDumpTable() {
            String hiveTableName = cfg.getString(KEY_HIVE_TAB_NAME);
            if (StringUtils.isBlank(hiveTableName)) {
                throw new IllegalStateException("config key " + KEY_HIVE_TAB_NAME + " can not be null");
            }
            if (!(writerPlugin instanceof DataXHiveWriter)) {
                throw new IllegalStateException("hiveWriterPlugin must be type of DataXHiveWriter");
            }
            return EntityName.create(getHiveWriterPlugin().getHiveConnGetter().getDbName(), hiveTableName);
        }

        private DataXHiveWriter getHiveWriterPlugin() {
            if (!(writerPlugin instanceof DataXHiveWriter)) {
                throw new IllegalStateException("hiveWriterPlugin must be type of DataXHiveWriter");
            }
            return (DataXHiveWriter) writerPlugin;
        }


        protected void initializeHiveTable(List<HiveColumn> cols) {
            try {
                try (Connection conn = getHiveWriterPlugin().getConnection()) {
                    Objects.requireNonNull(this.tabDumpParentPath, "tabDumpParentPath can not be null");
                    JoinHiveTask.initializeHiveTable(fileSystem, new HdfsPath(this.tabDumpParentPath), parseFSFormat()
                            , cols, colsExcludePartitionCols, conn, dumpTable, this.ptRetainNum);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private HdfsFormat parseFSFormat() {
            try {
                HdfsFormat fsFormat = new HdfsFormat();
                fsFormat.setFieldDelimiter((String) jobFieldDelimiter.get(this));
                fsFormat.setFileType(HdfsFileType.parse((String) jobFileType.get(this)));
                return fsFormat;
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        private List<HiveColumn> getCols() {
            try {
                List<Configuration> cols = (List<Configuration>) jobColumnsField.get(this);
                AtomicInteger index = new AtomicInteger();
                return cols.stream().map((c) -> {
                    HiveColumn hivCol = new HiveColumn();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            StringUtils.upperCase(c.getString(Key.TYPE)));
                    String name = StringUtils.remove(c.getString(Key.NAME), "`");
                    if (StringUtils.isBlank(name)) {
                        throw new IllegalStateException("col name can not be blank");
                    }
                    hivCol.setName(name);
                    hivCol.setType(columnType.name());
                    hivCol.setIndex(index.getAndIncrement());
                    return hivCol;
                }).collect(Collectors.toList());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public void post() {
            super.post();
            // 需要将刚导入的hdfs和hive的parition绑定
            // Set<EntityName> hiveTables = Collections.singleton(this.dumpTable);

            if (CollectionUtils.isEmpty(colsExcludePartitionCols)) {
                throw new IllegalStateException("table:" + this.dumpTable + " relevant colsExcludePartitionCols can not be empty");
            }
            Objects.requireNonNull(tabDumpParentPath, "tabDumpParentPath can not be null");


            this.bindHiveTables();
        }

        protected void bindHiveTables() {
            try {
                try (Connection hiveConn = getHiveWriterPlugin().getConnection()) {
                    BindHiveTableTool.bindHiveTables(fileSystem
                            , Collections.singletonMap(this.dumpTable, new Callable<BindHiveTableTool.HiveBindConfig>() {
                                @Override
                                public BindHiveTableTool.HiveBindConfig call() throws Exception {
                                    return new BindHiveTableTool.HiveBindConfig(colsExcludePartitionCols, tabDumpParentPath);
                                }
                            })
                            , this.dumpTimeStamp //
                            , new ITaskContext() {
                                @Override
                                public Connection getObj() {
                                    return hiveConn;
                                }
                            });
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
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
            setHdfsHelper(this.getPluginJobConf(), taskHdfsHelperField, getHdfsWriterPlugin(getPluginJobConf()), this);
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

    private static void setHdfsHelper(Configuration pluginJobConf, Field hdfsHelperField, DataXHdfsWriter hiveWriter, Object helperOwner) {
        try {
            TisExtendHdfsHelper hdfsHelper = new TisExtendHdfsHelper();

            //DataXHiveWriter hiveWriter = getHiveWriterPlugin(pluginJobConf);
            FileSystemFactory fs = hiveWriter.getFs();
            hdfsHelper.fileSystem = fs.getFileSystem().unwrap();
            org.apache.hadoop.conf.Configuration cfg = new org.apache.hadoop.conf.Configuration();
            cfg.setClassLoader(TIS.get().getPluginManager().uberClassLoader);

            org.apache.hadoop.mapred.JobConf conf = new JobConf(cfg);
            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, pluginJobConf.getString("defaultFS"));
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set(JobContext.WORKING_DIR, (new Path("/user/admin/dataxtest")).toString());
            hdfsHelper.conf = conf;
            hdfsHelperField.set(helperOwner, hdfsHelper);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static DataXHdfsWriter getHdfsWriterPlugin(Configuration cfg) {
        String dataxName = cfg.getString(DataxUtils.DATAX_NAME);
        DataxWriter dataxWriter = DataxWriter.load(null, dataxName);
        if (!(dataxWriter instanceof DataXHdfsWriter)) {
            throw new IllegalStateException("datax Writer must be type of 'DataXHiveWriter',but now is:" + dataxWriter.getClass());
        }
        return (DataXHdfsWriter) dataxWriter;
    }

    public static DataXHiveWriter getHiveWriterPlugin(Configuration pluginJobConf) {
        String dataxName = pluginJobConf.getString(DataxUtils.DATAX_NAME);
        DataxWriter dataxWriter = DataxWriter.load(null, dataxName);
        if (!(dataxWriter instanceof DataXHiveWriter)) {
            throw new IllegalStateException("datax Writer must be type of 'DataXHiveWriter',but now is:" + dataxWriter.getClass());
        }
        return (DataXHiveWriter) dataxWriter;
    }
}
