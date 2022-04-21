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

package com.alibaba.datax.plugin.writer.hudi;

import com.alibaba.datax.plugin.writer.hudi.log.LogbackBinder;
import com.qlangtech.tis.config.hive.IHiveConn;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.hdfs.test.HdfsFileSystemFactoryTestUtils;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TISCollectionUtils;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.plugin.datax.BasicFSWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-25 16:12
 **/
public class TISHoodieDeltaStreamer implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(TISHoodieDeltaStreamer.class);

    public static void main(String[] args) throws Exception {
        Enumeration allAppenders = LogManager.getRootLogger().getAllAppenders();
        int allAppendersCount = 0;
        while (allAppenders.hasMoreElements()) {
            allAppendersCount++;
        }
        System.out.println("===============allAppendersCount:" + allAppendersCount);
        String logbackPath = null;
        if (!Config.SYSTEM_KEY__LOGBACK_HUDI.equals(logbackPath = System.getProperty(Config.SYSTEM_KEY_LOGBACK_PATH_KEY))) {
            throw new IllegalStateException("system property '" + Config.SYSTEM_KEY_LOGBACK_PATH_KEY + "' is illegal,logbackPath:" + logbackPath);
        }
        LogManager.getRootLogger().addAppender(new HudiLoggerAppender());


        System.setProperty(Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS
                , String.valueOf(Boolean.TRUE.booleanValue()));
        CenterResource.setNotFetchFromCenterRepository();
        final HoodieDeltaStreamer.Config cfg = HoodieDeltaStreamer.getConfig(args);

        Map<String, String> additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
        JavaSparkContext jssc =
                UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster, additionalSparkConfigs);

        if (cfg.enableHiveSync) {
            LOG.warn("--enable-hive-sync will be deprecated in a future release; please use --enable-sync instead for Hive syncing");
        }
        String[] tabNames = StringUtils.split(cfg.targetTableName, "/");
        if (tabNames.length != 2) {
            throw new IllegalArgumentException("param targetTableName must seperate with '/'");
        }
        String dataName = tabNames[1];
        cfg.targetTableName = tabNames[0];

        setMockStub(dataName);

        BasicFSWriter writerPlugin = BasicFSWriter.getWriterPlugin(dataName);
        try {
            if (!(writerPlugin instanceof IHiveConn)) {
                throw new IllegalStateException("instance writerPlugin:"
                        + writerPlugin.getClass().getName() + " must be type of " + IHiveConn.class.getSimpleName());
            }
            Configuration hadoopCfg = jssc.hadoopConfiguration();
            FileSystem fs = writerPlugin.getFs().getFileSystem().unwrap();
            hadoopCfg.addResource(fs.getConf());
            hadoopCfg.set(HiveConf.ConfVars.METASTOREURIS.varname
                    , ((IHiveConn) writerPlugin).getHiveConnMeta().getMetaStoreUrls());
            new HoodieDeltaStreamer(cfg, jssc
                    , fs, jssc.hadoopConfiguration()).sync();
        } finally {
            jssc.stop();
        }
    }

    private static class HudiLoggerAppender extends AppenderSkeleton {
        //private final SimpleLayout layout = new SimpleLayout();
        private final ILoggerFactory loggerFactory;
        final String mdcCollection;
        final String taskId;

        public HudiLoggerAppender() {
            super();
            this.loggerFactory = LogbackBinder.getSingleton().getLoggerFactory();
            this.mdcCollection = System.getenv(TISCollectionUtils.KEY_COLLECTION);
            this.taskId = System.getenv(IParamContext.KEY_TASK_ID);
            if (StringUtils.isEmpty(taskId)) {
                throw new IllegalArgumentException("taskId can not be empty");
            }
        }

        @Override
        protected void append(LoggingEvent event) {

            MDC.put(IParamContext.KEY_TASK_ID, taskId);
            if (org.apache.commons.lang3.StringUtils.isNotEmpty(mdcCollection)) {
                MDC.put(TISCollectionUtils.KEY_COLLECTION, mdcCollection);
            }
            Level level = event.getLevel();
            if (level.isGreaterOrEqual(Level.INFO)) {
                Logger logger = this.loggerFactory.getLogger(event.getLoggerName());
                switch (level.toInt()) {
                    case Level.INFO_INT:
                        logger.info(event.getRenderedMessage());
                        break;
                    case Level.WARN_INT:
                        logger.warn(event.getRenderedMessage());
                        break;
                    case Level.ERROR_INT:
                    case Level.FATAL_INT:
                        ThrowableInformation tinfo = event.getThrowableInformation();
                        if (tinfo != null) {
                            logger.error(event.getRenderedMessage(), tinfo.getThrowable());
                        } else {
                            logger.error(event.getRenderedMessage());
                        }
                }
            }

            //  System.out.println(NetUtils.getHost() + ",loggerName:" + event.getLoggerName() + "---------------:" + layout.format(event));
        }

        @Override
        public void close() {

        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    }


    private static void setMockStub(String dataName) {
        if (HdfsFileSystemFactoryTestUtils.testDataXName.equalWithName(dataName)) {
            LOG.info("dataXName:" + dataName + " has match test phrase create test stub mock for DataxWriter");
            DataxWriter.dataxWriterGetter = (dataXname) -> {
                return new MockBasicFSWriter();
            };
        }
    }

    private static class MockBasicFSWriter extends BasicFSWriter implements IHiveConn {
        @Override
        public String getTemplate() {
            return null;
        }

        @Override
        public Class<?> getOwnerClass() {
            return BasicFSWriter.class;
        }

        @Override
        public FileSystemFactory getFs() {
            return HdfsFileSystemFactoryTestUtils.getFileSystemFactory();
        }

        @Override
        protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
            return null;
        }

        @Override
        public IHiveConnGetter getHiveConnMeta() {
            return HdfsFileSystemFactoryTestUtils.createHiveConnGetter();
        }
    }
}
