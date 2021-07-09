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
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriter;
import com.alibaba.datax.plugin.writer.hdfswriter.Key;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-08 17:47
 **/
public abstract class BasicHdfsWriterJob<T extends BasicFSWriter> extends HdfsWriter.Job {
    static final Field jobHdfsHelperField;
    static final Field taskHdfsHelperField;
    private static final Logger logger = LoggerFactory.getLogger(BasicHdfsWriterJob.class);

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

    protected Path tabDumpParentPath;

    private T writerPlugin = null;
    protected ITISFileSystem fileSystem = null;
    protected Configuration cfg = null;


    protected T getWriterPlugin() {
        return writerPlugin;
    }

    @Override
    public void init() {
        Configuration pluginJobConf = this.getPluginJobConf();
        Objects.requireNonNull(writerPlugin, "writerPlugin can not be null");
        super.init();

        BasicHdfsWriterJob.setHdfsHelper(pluginJobConf, BasicHdfsWriterJob.jobHdfsHelperField, writerPlugin, this);
    }

    @Override
    public void prepare() {
        super.prepare();
        // 需要自动创建hive表
        this.fileSystem = writerPlugin.getFs().getFileSystem();
        Objects.requireNonNull(fileSystem, "fileSystem can not be null");
    }

    @Override
    public Configuration getPluginJobConf() {
        this.cfg = super.getPluginJobConf();
        this.writerPlugin = TisDataXHiveWriter.getHdfsWriterPlugin(this.cfg);
        try {
            if (this.tabDumpParentPath == null) {
               // T writerPlugin = this.getWriterPlugin();
                Path path = createPath();
                Objects.requireNonNull(this.tabDumpParentPath, "tabDumpParentPath can not be null");
//                SimpleDateFormat timeFormat = new SimpleDateFormat(this.cfg.getNecessaryValue("ptFormat", HdfsWriterErrorCode.REQUIRED_VALUE));
//                this.dumpTimeStamp = timeFormat.format(new Date());
//                this.dumpTable = this.createDumpTable();
//                this.tabDumpParentPath = new Path(writerPlugin.getFs().getFileSystem().getRootDir(), getHdfsSubPath());
//                Path pmodPath = getPmodPath();
//                // 将path创建
                HdfsFileSystemFactory hdfsFactory = (HdfsFileSystemFactory) writerPlugin.getFs();
//                hdfsFactory.getFileSystem().mkdirs(new HdfsPath(pmodPath));
                //jobPath.set(this, pmodPath.toString());
                cfg.set(Key.PATH, path);
                logger.info("config param {}:{}", Key.PATH, hdfsFactory.hdfsAddress);
                cfg.set(Key.DEFAULT_FS, hdfsFactory.hdfsAddress);
            }
        } catch (Throwable e) {
            throw new BasicEngineJob.JobPropInitializeException("pmodPath initial error", e);
        }
        return this.cfg;
    }

    protected abstract Path createPath() throws IOException;

    static void setHdfsHelper(Configuration pluginJobConf, Field hdfsHelperField, BasicFSWriter hiveWriter, Object helperOwner) {
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
}
