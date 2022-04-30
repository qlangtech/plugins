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

package com.qlangtech.tis.plugin.datax.hudi;

import com.alibaba.datax.plugin.writer.hudi.HudiConfig;
import com.alibaba.datax.plugin.writer.hudi.TypedPropertiesBuilder;
import com.qlangtech.tis.config.hive.HiveUserToken;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TISCollectionUtils;
import com.qlangtech.tis.order.center.IParamContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Hudi 文件导入完成之后，开始执行同步工作
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-09 12:22
 **/
public class HudiDumpPostTask implements IRemoteTaskTrigger {


    private static Logger logger = LoggerFactory.getLogger(HudiDumpPostTask.class);

    private final HudiSelectedTab hudiTab;
    //private final List<String> dataXFileNames;
    // private final HudiTableMeta tabMeta;
    private final ISparkConnGetter sparkConnGetter;
    private final IHiveConnGetter hiveConnMeta;
    private final DataXHudiWriter hudiWriter;
    private final DataXCfgGenerator.GenerateCfgs generateCfgs;
    private final IExecChainContext execContext;


    private SparkAppHandle sparkAppHandle;

    public HudiDumpPostTask(IExecChainContext execContext, HudiSelectedTab hudiTab, DataXHudiWriter hudiWriter, DataXCfgGenerator.GenerateCfgs generateCfgs) {
        if (hudiTab == null) {
            throw new IllegalArgumentException("param tableName can not be empty");
        }
        this.hudiTab = hudiTab;
        this.execContext = execContext;
        //  this.dataXFileNames = dataXFileNames;
        // this.tabMeta = tabMeta;
        this.sparkConnGetter = hudiWriter.getSparkConnGetter();
        this.hiveConnMeta = hudiWriter.getHiveConnMeta();
        this.hudiWriter = hudiWriter;
        this.generateCfgs = generateCfgs;

    }

    public static IPath createTabDumpParentPath(ITISFileSystem fs, IPath tabDumpDir) {
        Objects.requireNonNull(fs, "ITISFileSystem can not be null");
        //IPath tabDumpDir = getDumpDir();
        return fs.getPath(tabDumpDir, "data");
    }

    @Override
    public List<String> getTaskDependencies() {
//        File dataXWorkDir = IDataxProcessor.getDataXWorkDir(null, this.hudiWriter.dataXName);
//        DataXCfgGenerator.GenerateCfgs generateCfgs = DataXCfgGenerator.GenerateCfgs.readFromGen(dataXWorkDir);
//        return generateCfgs.getGroupedChildTask().get(tableName);
        return this.generateCfgs.getDataXTaskDependencies(hudiTab.getName());
    }

    @Override
    public String getTaskName() {
        return "hudi_delta_" + this.hudiTab.getName();
    }

    @Override
    public void run() {

        ITISFileSystem fs = this.hudiWriter.getFs().getFileSystem();
        IPath dumpDir = HudiTableMeta.getDumpDir(fs, this.hudiTab.getName(), execContext.getPartitionTimestamp(), this.hiveConnMeta);
        IPath fsSourcePropsPath = fs.getPath(dumpDir, "meta/" + this.hudiTab.getName() + "-source.properties");


        try {
            this.writeSourceProps(fs, dumpDir, fsSourcePropsPath);
            SparkAppHandle handle = this.launchSparkRddConvert(fs, dumpDir, fsSourcePropsPath);
            if (handle != null) {
                try {
                    handle.stop();
                } catch (Throwable e) { }
            }
        } catch (Throwable e) {
            if (this.sparkAppHandle != null) {
                try {
                    this.sparkAppHandle.kill();
                } catch (Throwable ex) {
                    logger.warn(ex.getMessage(), ex);
                }
            }
            throw new RuntimeException(e);
        }
    }

    private SparkAppHandle launchSparkRddConvert(ITISFileSystem fs, IPath dumpDir, IPath fsSourcePropsPath) throws Exception {

        Map<String, String> env = Config.getInstance().getAllKV();

        String mdcCollection = MDC.get(TISCollectionUtils.KEY_COLLECTION);
        final String taskId = MDC.get(IParamContext.KEY_TASK_ID);
        if (StringUtils.isEmpty(taskId)) {
            throw new IllegalStateException("mdc param taskId can not be null");
        }
        env.put(IParamContext.KEY_TASK_ID, taskId);
        if (StringUtils.isNotEmpty(mdcCollection)) {
            env.put(TISCollectionUtils.KEY_COLLECTION, mdcCollection);
        }

        logger.info("environment props ===========================");
        for (Map.Entry<String, String> entry : env.entrySet()) {
            logger.info("key:{},value:{}", entry.getKey(), entry.getValue());
        }
        logger.info("=============================================");
        SparkLauncher handle = new SparkLauncher(env);
       // handle.directory();
//        File logFile = new File(TisAppLaunchPort.getAssebleTaskDir(), "full-" + taskId + ".log");
//        FileUtils.touch(logFile);
//        handle.redirectError(logFile);
        // 测试用
//        handle.redirectError(new File("error.log"));
        //  handle.redirectToLog(DataXHudiWriter.class.getName());
        // String tabName = this.getFileName();

        File hudiDependencyDir = HudiConfig.getHudiDependencyDir();
        File sparkHome = HudiConfig.getSparkHome();

        File resJar = FileUtils.listFiles(hudiDependencyDir, new String[]{"jar"}, false)
                .stream().findFirst().orElseThrow(
                        () -> new IllegalStateException("must have resJar hudiDependencyDir:" + hudiDependencyDir.getAbsolutePath()));

        File addedJars = new File(hudiDependencyDir, "lib");
        boolean[] hasAddJar = new boolean[1];
        FileUtils.listFiles(addedJars, new String[]{"jar"}, false).forEach((jar) -> {
            handle.addJar(String.valueOf(jar.toPath().normalize()));
            hasAddJar[0] = true;
        });
        if (!hasAddJar[0]) {
            throw new IllegalStateException("path must contain jars:" + addedJars.getAbsolutePath());
        }
        handle.setAppResource(String.valueOf(resJar.toPath().normalize()));
        // ISparkConnGetter sparkConnGetter = writerPlugin.getSparkConnGetter();
        handle.setMaster(sparkConnGetter.getSparkMaster());
        handle.setSparkHome(String.valueOf(sparkHome.toPath().normalize()));
        handle.setMainClass("com.alibaba.datax.plugin.writer.hudi.TISHoodieDeltaStreamer");


        handle.addAppArgs("--table-type", this.hudiWriter.getHudiTableType().getValue()
                , "--source-class", "org.apache.hudi.utilities.sources.AvroDFSSource"
                , "--source-ordering-field", hudiTab.sourceOrderingField
                , "--target-base-path", String.valueOf(HudiTableMeta.getHudiDataDir(fs, dumpDir))
                , "--target-table", this.hudiTab.getName() + "/" + hudiWriter.dataXName
                , "--props", String.valueOf(fsSourcePropsPath)
                , "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
                , "--enable-sync"
        );

        if (hudiWriter.getHudiTableType() == HudiWriteTabType.MOR) {
            handle.addAppArgs("--disable-compaction");
        }
        // https://hudi.apache.org/docs/tuning-guide/

        StringBuffer javaOpts = new StringBuffer("-D" + Config.SYSTEM_KEY_LOGBACK_PATH_KEY + "=" + Config.SYSTEM_KEY__LOGBACK_HUDI);
        javaOpts.append(" -D" + Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS + "=true");


        if (Boolean.getBoolean(KEY_DELTA_STREM_DEBUG)) {
            // 测试中使用
            javaOpts.append(" -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=18888");
        }

        handle.setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, javaOpts.toString()
        );
//        handle.setConf(SparkLauncher.DRIVER_MEMORY, "4G");
//        handle.setConf(SparkLauncher.EXECUTOR_MEMORY, "6G");
//        handle.addSparkArg("--driver-memory", "1024M");
//        handle.addSparkArg("--executor-memory", "2G");

        this.hudiWriter.sparkSubmitParam.setHandle(handle);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        this.sparkAppHandle = handle.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                SparkAppHandle.State state = sparkAppHandle.getState();
                if (state.isFinal()) {
                    // finalState[0] = state;
                    System.out.println("Info:" + state + ",appId:" + sparkAppHandle.getAppId());
                    countDownLatch.countDown();
                }
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                System.out.println("Info:" + sparkAppHandle.getState().toString());
            }
        });

        countDownLatch.await();
        if (sparkAppHandle.getState() != SparkAppHandle.State.FINISHED) {
            throw new TisException("spark app:" + sparkAppHandle.getAppId()
                    + " execute result not successful:" + sparkAppHandle.getState());
        }
        return sparkAppHandle;
    }


    @Override
    public void cancel() {
        try {
            sparkAppHandle.stop();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private void writeSourceProps(ITISFileSystem fs, IPath dumpDir, IPath fsSourcePropsPath) {


        IPath sourceSchemaPath = HudiTableMeta.getTableSourceSchema(fs, dumpDir);// HudiTableMeta.createFsSourceSchema(fs, this.hudiTab.getName(), dumpDir, this.hudiTab);
        if (!fs.exists(sourceSchemaPath)) {
            throw new IllegalStateException("sourceSchemaPath:" + sourceSchemaPath.getName() + " is not exist");
        }
        IPath tabDumpParentPath = createTabDumpParentPath(fs, dumpDir);
        // 写csv文件属性元数据文件

        try (OutputStream write = fs.create(fsSourcePropsPath, true)) {
            // TypedProperties props = new TypedProperties();
            TypedPropertiesBuilder props = new TypedPropertiesBuilder();

            String shuffleParallelism = String.valueOf(this.hudiWriter.shuffleParallelism);
            props.setProperty("hoodie.upsert.shuffle.parallelism", shuffleParallelism);
            props.setProperty("hoodie.insert.shuffle.parallelism", (shuffleParallelism));
            props.setProperty("hoodie.delete.shuffle.parallelism", (shuffleParallelism));
            props.setProperty("hoodie.bulkinsert.shuffle.parallelism", (shuffleParallelism));
            props.setProperty("hoodie.embed.timeline.server", "false");
            props.setProperty("hoodie.filesystem.view.type", "EMBEDDED_KV_STORE");

            // @see HoodieCompactionConfig.INLINE_COMPACT
            // props.setProperty("hoodie.compact.inline", (hudiTabType == HudiWriteTabType.MOR) ? "true" : "false");
            // BasicFSWriter writerPlugin = this.getWriterPlugin();
//https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
            props.setProperty("hoodie.deltastreamer.source.dfs.root", String.valueOf(tabDumpParentPath));
//            props.setProperty("hoodie.deltastreamer.csv.header", Boolean.toString(CSVWriter.CSV_FILE_USE_HEADER));
//            props.setProperty("hoodie.deltastreamer.csv.sep", String.valueOf(CSVWriter.CSV_Column_Separator));
//            props.setProperty("hoodie.deltastreamer.csv.nullValue", CSVWriter.CSV_NULL_VALUE);
//            props.setProperty("hoodie.deltastreamer.csv.escape", String.valueOf(CSVWriter.CSV_ESCAPE_CHAR));
            //  props.setProperty("hoodie.deltastreamer.csv.escapeQuotes", "false");


            props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", String.valueOf(sourceSchemaPath));
            props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", String.valueOf(sourceSchemaPath));

            // please reference: DataSourceWriteOptions , HiveSyncConfig
            final IHiveConnGetter hiveMeta = this.hudiWriter.getHiveConnMeta();
            props.setProperty("hoodie.datasource.hive_sync.database", hiveMeta.getDbName());
            props.setProperty("hoodie.datasource.hive_sync.table", this.hudiTab.getName());

            if (this.hudiTab.partition == null) {
                throw new IllegalStateException("hudiPlugin.partitionedBy can not be empty");
            }

            this.hudiTab.partition.setProps(props, this.hudiWriter);
//            props.setProperty("hoodie.datasource.hive_sync.partition_fields", hudiPlugin.partitionedBy);
//            // "org.apache.hudi.hive.MultiPartKeysValueExtractor";
//            // partition 分区值抽取类
//            props.setProperty("hoodie.datasource.hive_sync.partition_extractor_class"
//                    , "org.apache.hudi.hive.MultiPartKeysValueExtractor");

            Optional<HiveUserToken> hiveUserToken = hiveMeta.getUserToken();
            if (hiveUserToken.isPresent()) {
                HiveUserToken token = hiveUserToken.get();
                props.setProperty("hoodie.datasource.hive_sync.username", token.userName);
                props.setProperty("hoodie.datasource.hive_sync.password", token.password);
            }
            props.setProperty("hoodie.datasource.hive_sync.jdbcurl", hiveMeta.getJdbcUrl());
            props.setProperty("hoodie.datasource.hive_sync.mode", "jdbc");

            props.setProperty("hoodie.datasource.write.recordkey.field", this.hudiTab.recordField);
            //  props.setProperty("hoodie.datasource.write.partitionpath.field", hudiWriter.partitionedBy);

            props.store(write);

        } catch (IOException e) {
            throw new RuntimeException("faild to write " + tabDumpParentPath + " CSV file metaData", e);
        }
    }

}
