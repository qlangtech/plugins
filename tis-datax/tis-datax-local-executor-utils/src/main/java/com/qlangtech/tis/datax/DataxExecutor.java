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
package com.qlangtech.tis.datax;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.IDataXCfg;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TaskSoapUtils;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.order.center.IAppSourcePipelineController;
import com.qlangtech.tis.realtime.transfer.TableSingleDataIndexStatus;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.realtime.yarn.rpc.MasterJob;
import com.qlangtech.tis.realtime.yarn.rpc.UpdateCounterMap;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 执行DataX任务入口
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2021-04-20 12:38
 */
public class DataxExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DataxExecutor.class);

    /**
     * 当使用dolphinscheduler运行数据同步任务希望在本地记录一份完整的dataX执行日志，而不是都要通过grpc的方式传输到assemble节点查看
     *
     * @param filePath
     */
    private static void initSpecialLocalLoggerFileAppender(String filePath) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        // 创建一个指定文件路径的fileAppender
        FileAppender<ILoggingEvent> localFileAppender = new FileAppender<>();
        localFileAppender.setContext(loggerContext);
        localFileAppender.setName("localFile");
        localFileAppender.setFile(filePath);
        localFileAppender.setPrudent(true);
        localFileAppender.setAppend(true);
        localFileAppender.setImmediateFlush(true);

        // 创建一个PatternLayoutEncoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        //   %-5level %logger{36}-%msg%n
        encoder.setPattern("%d{MM-dd HH:mm:ss} %-5level %logger{36}-%msg%n");
        encoder.start();

        // 将Encoder添加到Appender中
        localFileAppender.setEncoder(encoder);
        localFileAppender.start();
        // 获取根logger
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(localFileAppender);
    }

    private static final int ASSERT_PARAM_LENGTH = 10;

    /**
     * @param args
     * @see //DataxPrePostConsumer
     * @see //DataXJobSingleProcessorExecutor
     * 入口开始执行
     */
    public static void main(String[] args) throws Exception {
        assertParamsLength(args);

        Integer jobId = Integer.parseInt(args[0]);
        DataXJobInfo jobInfo = DataXJobInfo.parse(args[1]);
        String dataXName = args[2];
        String incrStateCollectAddress = args[3];
        DataXJobSubmit.InstanceType execMode = DataXJobSubmit.InstanceType.parse(args[4]);

        final int allRows = Integer.parseInt(args[5]);

        StoreResourceType resType = StoreResourceType.parse(args[6]);

        final int taskSerializeNum = Integer.parseInt(args[7]);

        final long execEpochMilli = Long.parseLong(args[8]);
//        boolean isDisableGrpcRemoteServerConnect;
//        if (isDisableGrpcRemoteServerConnect = Boolean.parseBoolean(args[9])) {
//            ITISCoordinator.disableRemoteServer();
//        }
//        logger.info("isDisableGrpcRemoteServerConnect:{}", isDisableGrpcRemoteServerConnect);

        boolean isDisableGrpcRemoteServerConnect = Boolean.parseBoolean(args[9]);

        String localLoggerFilePath = null;
        if (StringUtils.isNotEmpty(localLoggerFilePath = System.getProperty(Config.EXEC_LOCAL_LOGGER_FILE_PATH))) {
            initSpecialLocalLoggerFileAppender(localLoggerFilePath);
        }

        // 任务每次执行会生成一个时间戳
        // final String execTimeStamp = args[6];
        //configuration.set(DataxUtils.EXEC_TIMESTAMP, args.execTimeStamp);
        if (StringUtils.isEmpty(System.getProperty(DataxUtils.EXEC_TIMESTAMP))) {
            throw new IllegalArgumentException("system prop '" + DataxUtils.EXEC_TIMESTAMP + "' can not be empty");
        }

        JobCommon.setMDC(jobId, dataXName);
        Objects.requireNonNull(jobInfo, "arg 'jobName' can not be null");
        //        if () {
        //            throw new IllegalArgumentException("arg 'jobName' can not be null");
        //        }
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("arg 'dataXName' can not be null");
        }
        if (StringUtils.isEmpty(incrStateCollectAddress)) {
            throw new IllegalArgumentException("arg 'incrStateCollectAddress' can not be null");
        }

        RpcServiceReference statusRpc = StatusRpcClientFactory.getService(
                ITISCoordinator.create(!isDisableGrpcRemoteServerConnect, Optional.of(incrStateCollectAddress)));
        AssembleSvcCompsite.statusRpc = statusRpc;

        //  final AtomicReference<ITISRpcService> rpcRef = new AtomicReference<>(statusRpc);

        DataxExecutor dataxExecutor = new DataxExecutor(statusRpc, execMode, allRows);

        if (execMode == DataXJobSubmit.InstanceType.DISTRIBUTE) {
            // 如果是分布式执行状态，需要通过RPC的方式来监听监工是否执行了客户端终止操作
            Object thread = monitorDistributeCommand(jobId, jobInfo, dataXName, statusRpc, dataxExecutor);
            Objects.requireNonNull(thread);
            // DataxExecutor.synchronizeDataXPluginsFromRemoteRepository(dataXName, resType, jobInfo);
        }

        try {
            dataxExecutor.reportDataXJobStatus(false, false, false, jobId, jobInfo);
            IDataxProcessor dataxProcessor = DataxProcessor.load(null, resType, dataXName);
            //  File jobPath = jobInfo.getJobPath(dataxProcessor.getDataxCfgDir(null));
            DataXJobArgs jobArgs = DataXJobArgs.createJobArgs(dataxProcessor, jobId, jobInfo, taskSerializeNum,
                    execEpochMilli);

            dataxExecutor.exec(jobInfo, dataxProcessor, jobArgs);
            dataxExecutor.reportDataXJobStatus(false, jobId, jobInfo);
        } catch (Throwable e) {
            dataxExecutor.reportDataXJobStatus(true, jobId, jobInfo);
            logger.error(e.getMessage(), e);
            try {
                //确保日志向远端写入了
                Thread.sleep(3000);
            } catch (InterruptedException ex) {

            }
            System.exit(1);
            return;
        }
        logger.info("dataX:" + dataXName + ",taskid:" + jobId + " finished");
        System.exit(0);
    }

    private static void assertParamsLength(String[] args) {
        if (args.length != ASSERT_PARAM_LENGTH) {
            throw new IllegalArgumentException("args length must be " + ASSERT_PARAM_LENGTH + ",but now is " + args.length);
        }
    }

    private static Thread monitorDistributeCommand(Integer jobId, DataXJobInfo jobInfo, String dataXName,
                                                   RpcServiceReference statusRpc, DataxExecutor dataxExecutor) {
        Thread overseerListener = new Thread() {
            @Override
            public void run() {
                UpdateCounterMap status = new UpdateCounterMap();
                status.setFrom(NetUtils.getHost());
                logger.info("start to listen the dataX job taskId:{},jobName:{},dataXName:{} overseer cancel", jobId, jobInfo, dataXName);
                TableSingleDataIndexStatus dataXStatus = new TableSingleDataIndexStatus();
                dataXStatus.setUUID(jobInfo.jobFileName);
                status.addTableCounter(IAppSourcePipelineController.DATAX_FULL_PIPELINE + dataXName, dataXStatus);

                while (true) {
                    status.setUpdateTime(System.currentTimeMillis());
                    MasterJob masterJob =
                            (statusRpc).reportStatus(status);
                    if (masterJob != null && masterJob.isStop()) {
                        logger.info("datax job:{},taskid:{} has received an CANCEL signal", jobInfo, jobId);
                        dataxExecutor.reportDataXJobStatus(true, jobId, jobInfo);
                        System.exit(DataXJobInfo.DATAX_THREAD_PROCESSING_CANCAL_EXITCODE);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        };
        overseerListener.setUncaughtExceptionHandler((thread, e) -> logger.error("jobId:" + jobId + ",jobName:" + jobInfo, e));
        overseerListener.start();
        return overseerListener;
    }

    public void exec(DataXJobInfo jobName, IDataxProcessor processor, DataXJobArgs jobArgs) throws Exception {
        final JarLoader uberClassLoader = new TISJarLoader(TIS.get().getPluginManager());
        LoadUtil.cleanJarLoaderCenter();
        this.exec(uberClassLoader, jobName, processor, jobArgs);
    }

    /**
     * new DataXJobArgs(jobPath, jobId, "standalone", taskSerializeNum)
     *
     * @param uberClassLoader
     * @param jobName
     * @param dataxProcessor
     * @throws Exception
     */
    public void exec(final JarLoader uberClassLoader, DataXJobInfo jobName, IDataxProcessor dataxProcessor,
                     DataXJobArgs jobArgs) throws Exception {
        if (uberClassLoader == null) {
            throw new IllegalArgumentException("param uberClassLoader can not be null");
        }
        //        if (StringUtils.isEmpty(dataxName)) {
        //            throw new IllegalArgumentException("param dataXName can not be null");
        //        }
        boolean success = false;
        JobCommon.setMDC(jobArgs.jobId, dataxProcessor.identityValue());
        try {
            logger.info("process DataX job,jobid:{},jobName:{}", jobArgs.jobId, jobName);
            //KeyedPluginStore.StoreResourceType resType = null;

            // IDataxProcessor dataxProcessor = DataxProcessor.load(null, resType, dataxName);
            this.startWork(jobName, dataxProcessor, uberClassLoader, jobArgs);
            success = true;
        } finally {
            TIS.clean(false);
            if (execMode == DataXJobSubmit.InstanceType.DISTRIBUTE) {
                try {
                    TaskSoapUtils.feedbackAsynTaskStatus(jobArgs.jobId, jobName.jobFileName, success);
                } catch (Throwable e) {
                    logger.warn("notify exec result faild,jobId:" + jobArgs.jobId + ",jobName:" + jobName, e);
                }
            }
        }
    }


    private static final MessageFormat FormatKeyPluginReader = new MessageFormat("plugin.reader.{0}");
    private static final MessageFormat FormatKeyPluginWriter = new MessageFormat("plugin.writer.{0}");

    private IDataXPluginMeta.DataXMeta readerMeta;
    private IDataXPluginMeta.DataXMeta writerMeta;

    private DataXJobSubmit.InstanceType execMode;
    private final int allRowsApproximately;
    private final long[] allReadApproximately = new long[1];
    private final RpcServiceReference statusRpc;

    public DataxExecutor(RpcServiceReference statusRpc, DataXJobSubmit.InstanceType execMode, int allRows) {
        this.statusRpc = Objects.requireNonNull(statusRpc, "statusRpc can not be null");
        this.execMode = execMode;
        this.allRowsApproximately = allRows;
    }


    /**
     * 开始执行数据同步任务
     *
     * @throws IOException
     * @throws Exception
     */
    public void startWork(DataXJobInfo jobName, IDataxProcessor dataxProcessor, final JarLoader uberClassLoader,
                          DataXJobArgs jobArgs) throws IOException, Exception {
        try {

            final String processName = dataxProcessor.identityValue();
            Objects.requireNonNull(dataxProcessor, "dataxProcessor can not be null");

            IDataxReader reader = DataXJobSubmit.getDataXJobInfo(jobName.getDbFactoryId(), (p) -> {
                return p.getRight();
            }, dataxProcessor.getReaders(null));


            IDataxWriter writer = dataxProcessor.getWriter(null);


            Objects.requireNonNull(reader, "dataxName:" + processName + " relevant reader can not be null");
            Objects.requireNonNull(writer, "dataxName:" + processName + " relevant writer can not be null");

            this.readerMeta = reader.getDataxMeta();
            this.writerMeta = writer.getDataxMeta();
            Objects.requireNonNull(readerMeta, "readerMeta can not be null");
            Objects.requireNonNull(writerMeta, "writerMeta can not be null");

            initializeClassLoader(Sets.newHashSet(this.getPluginReaderKey(), this.getPluginWriterKey()),
                    uberClassLoader);


            entry(jobArgs, jobName, dataxProcessor.getResType());

        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            cleanPerfTrace();
        }

    }

    public static void initializeClassLoader(Set<String> pluginKeys, JarLoader classLoader) throws IllegalAccessException {
        //        Map<String, JarLoader> jarLoaderCenter = (Map<String, JarLoader>) jarLoaderCenterField.get(null);
        //        jarLoaderCenter.clear();
        //
        //        for (String pluginKey : pluginKeys) {
        //            jarLoaderCenter.put(pluginKey, classLoader);
        //        }
        //        Objects.requireNonNull(jarLoaderCenter, "jarLoaderCenter can not be null");
        LoadUtil.initializeJarClassLoader(pluginKeys, classLoader);
    }

    public void reportDataXJobStatus(boolean faild, Integer taskId, DataXJobInfo jobName) {
        reportDataXJobStatus(faild, true, false, taskId, jobName);
    }

    public void reportDataXJobStatus(boolean faild, boolean complete, boolean waiting, Integer taskId,
                                     DataXJobInfo jobName) {
        // StatusRpcClientFactory.AssembleSvcCompsite svc = this.statusRpc.get();
        int readed = (int) allReadApproximately[0];
        boolean success = (complete && !faild);
        this.statusRpc.reportDumpJobStatus(faild, complete, waiting, taskId, jobName.jobFileName, readed, (success ? readed :
                this.allRowsApproximately));
    }

    public static class DataXJobArgs {
        private final File jobPath;
        private final Integer jobId;
        private final String runtimeMode;
        private final int taskSerializeNum;
        private final long execEpochMilli;
        //        private final String execTimeStamp;

        public DataXJobArgs(File jobPath, Integer jobId, String runtimeMode, int taskSerializeNum,
                            long execEpochMilli) {
            this.jobPath = jobPath;
            this.jobId = jobId;
            this.runtimeMode = runtimeMode;
            this.taskSerializeNum = taskSerializeNum;
            this.execEpochMilli = execEpochMilli;
            //            if (StringUtils.isEmpty(execTimeStamp)) {
            //                throw new IllegalArgumentException("param execTimeStamp can not be empty");
            //            }
            //            this.execTimeStamp = execTimeStamp;
        }

        public static DataXJobArgs createJobArgs(IDataxProcessor dataxProcessor, Integer jobId, DataXJobInfo jobInfo,
                                                 final int taskSerializeNum, final long execEpochMilli) {
            File jobPath = jobInfo.getJobPath(dataxProcessor.getDataxCfgDir(null));
            DataXJobArgs jobArgs = new DataXJobArgs(jobPath, jobId, "standalone", taskSerializeNum, execEpochMilli);
            return jobArgs;
        }

        public int getTaskSerializeNum() {
            return this.taskSerializeNum;
        }

        public long getExecEpochMilli() {
            return this.execEpochMilli;
        }

        @Override
        public String toString() {
            return "{" + "jobPath=" + jobPath.getAbsolutePath() + ", jobId=" + jobId + ", runtimeMode='" + runtimeMode + '\'' + '}';
        }
    }

    public void entry(DataXJobArgs args, DataXJobInfo jobName, StoreResourceType resType) throws Throwable {
        Pair<Configuration, IDataXNameAware> cfg = parse(args, resType, jobName);
        Configuration configuration = cfg.getLeft();
        logger.info("exec params:{}", args.toString());
        Objects.requireNonNull(configuration, "configuration can not be null");
        //  int jobId = args.jobId;

        boolean isStandAloneMode = "standalone".equalsIgnoreCase(args.runtimeMode);
        if (!isStandAloneMode && args.jobId == -1L) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "非 standalone 模式必须在 URL 中提供有效的 " + "jobId.");
        }

        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            logger.info(vmInfo.toString());
        }

        logger.info("\n" + filterJobConfiguration(configuration) + "\n");
        logger.debug(configuration.toJSON());
        ConfigurationValidate.doValidate(configuration);
        startEngine(cfg, args, jobName);

    }

    protected void startEngine(Pair<Configuration, IDataXNameAware> cfg, DataXJobArgs args, DataXJobInfo jobName) {

        Engine engine = new Engine() {
            @Override
            protected JobContainer createJobContainer(Configuration allConf) {
                return new TISDataXJobContainer(cfg.getRight(), allConf, args, jobName);
            }
        };
        AbstractContainer dataXContainer = engine.start(cfg.getLeft());
        setAllReadApproximately(dataXContainer.getContainerCommunicator().collect());
    }

    //    public static final String connectKeyParameter = "parameter";
    //
    //    static final String readerKeyPrefix = "job.content[0].reader." + connectKeyParameter + ".";
    //    static final String writerKeyPrefix = "job.content[0].writer." + connectKeyParameter + ".";

    private class TISDataXJobContainer extends JobContainer {
        private final Integer jobId;
        private final DataXJobInfo jobName;
        private final DataXJobArgs jobArgs;
        private final IDataXNameAware dataXName;

        public TISDataXJobContainer(IDataXNameAware dataXName, Configuration configuration, DataXJobArgs args,
                                    DataXJobInfo jobName) {
            super(configuration);
            this.jobArgs = args;
            this.jobId = Objects.requireNonNull(args.jobId);
            this.jobName = jobName;
            this.dataXName = dataXName;
        }

        @Override
        public Integer getTaskId() {
            return this.jobId;
        }

        @Override
        public String getFormatTime(TimeFormat format) {
            return format.format(jobArgs.getExecEpochMilli());
        }

        @Override
        public int getTaskSerializeNum() {
            return this.jobArgs.getTaskSerializeNum();
        }

        @Override
        public DataXName getTISDataXName() {
            return this.dataXName.getTISDataXName();
        }


        @Override
        public String getDataXName() {
            return this.dataXName.getTISDataXName().getPipelineName();
        }

        @Override
        protected StandAloneJobContainerCommunicator createContainerCommunicator(Configuration configuration) {
            return new StandAloneJobContainerCommunicator(configuration) {
                @Override
                public void report(Communication communication) {
                    super.report(communication);
                    setAllReadApproximately(communication);
                    reportDataXJobStatus(false, false, false, jobId, jobName);
                }
            };
        }
    }


    public static void setResType(Configuration configuration, StoreResourceType resType) {
        if (configuration == null) {
            throw new IllegalArgumentException("param configuration can not be null");
        }
        if (resType == null) {
            throw new IllegalArgumentException("param resType can not be null");
        }
        configuration.set(IDataXCfg.readerKeyPrefix + StoreResourceType.KEY_STORE_RESOURCE_TYPE, resType.getType());
        configuration.set(IDataXCfg.writerKeyPrefix + StoreResourceType.KEY_STORE_RESOURCE_TYPE, resType.getType());
    }

    private void setAllReadApproximately(Communication communication) {
        allReadApproximately[0] = communication.getLongCounter(CommunicationTool.TOTAL_READ_RECORDS);
    }

    /**
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     */
    private Pair<Configuration, IDataXNameAware> parse(DataXJobArgs args, StoreResourceType resType,
                                                       DataXJobInfo jobName) {
        final String jobPath = args.jobPath.getAbsolutePath();
        Configuration configuration = ConfigParser.parseJobConfig(jobPath);

        Configuration readerCfg = Configuration.newDefault();
        readerCfg.set("class", this.readerMeta.getImplClass());
        Configuration writerCfg = Configuration.newDefault();
        writerCfg.set("class", this.writerMeta.getImplClass());
        configuration.set(getPluginReaderKey(), readerCfg);
        configuration.set(getPluginWriterKey(), writerCfg);


        final String dataXKey = "job.content[0]." + StoreResourceType.DATAX_NAME;
        final String dataxName = configuration.getString(dataXKey);
        if (StringUtils.isEmpty(dataxName)) {
            throw new IllegalStateException("param " + dataXKey + " can not be null");
        }


        //        configuration.set(readerKeyPrefix + StoreResourceType.DATAX_NAME, dataxName);
        //        configuration.set(writerKeyPrefix + StoreResourceType.DATAX_NAME, dataxName);

        final String readerDbFactoryId = jobName.getDbFactoryId().identityValue();
        configuration.set(IDataXCfg.readerKeyPrefix + DataxUtils.DATASOURCE_FACTORY_IDENTITY, readerDbFactoryId);

        setResType(configuration, resType);
        //        configuration.set(readerKeyPrefix + StoreResourceType.KEY_STORE_RESOURCE_TYPE, resType.getType());
        //        configuration.set(writerKeyPrefix + StoreResourceType.KEY_STORE_RESOURCE_TYPE, resType.getType());


        //KeyedPluginStore.StoreResourceType.

        String readerPluginName = configuration.getString("job.content[0].reader.name");
        String writerPluginName = configuration.getString("job.content[0].writer.name");

        DataXCfgGenerator.validatePluginName(writerMeta, readerMeta, writerPluginName, readerPluginName);

        Configuration coreCfg = Configuration.from(IOUtils.loadResourceFromClasspath(DataxExecutor.class, "core.json"));
        coreCfg.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, args.jobId);

        configuration.merge(coreCfg,
                //ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH),
                false);


        Objects.requireNonNull(configuration.get(getPluginReaderKey()), FormatKeyPluginReader + " can not be null");
        Objects.requireNonNull(configuration.get(getPluginWriterKey()), FormatKeyPluginWriter + " can not be null");
        return Pair.of(configuration, () -> new DataXName(dataxName, resType));
        // todo config优化，只捕获需要的plugin
    }

    private String getPluginReaderKey() {
        Objects.requireNonNull(readerMeta, "readerMeta can not be null");
        return FormatKeyPluginReader.format(new String[]{readerMeta.getName()});
    }


    private String getPluginWriterKey() {
        Objects.requireNonNull(writerMeta, "writerMeta can not be null");
        return FormatKeyPluginWriter.format(new String[]{writerMeta.getName()});
    }


    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration job = configuration.getConfiguration("job");
        if (job == null) {
            throw new IllegalStateException("job relevant info can not be null,\n" + configuration.toJSON());
        }
        Configuration jobConfWithSetting = job.clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive =
                    StringUtils.endsWithIgnoreCase(key, "password") || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }

    private void cleanPerfTrace() {
        try {
            Field istField = PerfTrace.class.getDeclaredField("instance");
            istField.setAccessible(true);

            istField.set(null, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
