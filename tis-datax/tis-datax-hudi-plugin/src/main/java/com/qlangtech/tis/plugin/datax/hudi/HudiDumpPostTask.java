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
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TISCollectionUtils;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.web.start.TisAppLaunchPort;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Hudi 文件导入完成之后，开始执行同步工作
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-09 12:22
 **/
public class HudiDumpPostTask implements IRemoteTaskTrigger {

    private static Logger logger = LoggerFactory.getLogger(HudiDumpPostTask.class);

    private final String tableName;
    private final List<String> dataXFileNames;
    private final HudiTableMeta tabMeta;
    private final ISparkConnGetter sparkConnGetter;
    private final IHiveConnGetter hiveConnMeta;
    private final DataXHudiWriter hudiWriter;

    public HudiDumpPostTask(String tableName, HudiTableMeta tabMeta, DataXHudiWriter hudiWriter, List<String> dataXFileNames) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName can not be empty");
        }
        this.tableName = tableName;
        this.dataXFileNames = dataXFileNames;
        this.tabMeta = tabMeta;
        this.sparkConnGetter = hudiWriter.getSparkConnGetter();
        this.hiveConnMeta = hudiWriter.getHiveConnMeta();
        this.hudiWriter = hudiWriter;
    }

    @Override
    public String getTaskName() {
        return this.tableName;
    }

    @Override
    public RunningStatus getRunningStatus() {
       throw new UnsupportedOperationException();
    }

    @Override
    public void run() {

        try {
            this.launchSparkRddConvert();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void launchSparkRddConvert() throws Exception {

        // HashMap env = new HashMap();
        Map<String, String> env = Config.getInstance().getAllKV();


        String mdcCollection = MDC.get(TISCollectionUtils.KEY_COLLECTION);
        final String taskId = MDC.get(IParamContext.KEY_TASK_ID);
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
        File logFile = new File(TisAppLaunchPort.getAssebleTaskDir(), "full-" + taskId + ".log");
        FileUtils.touch(logFile);
        handle.redirectError(logFile);
        //  handle.redirectError(new File("error.log"));
        // handle.redirectToLog(DataXHudiWriter.class.getName());
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

        IPath fsSourcePropsPath = getSourcePropsPath();

        handle.addAppArgs("--table-type", this.tabMeta.getHudiTabType().getValue()
                , "--source-class", "org.apache.hudi.utilities.sources.CsvDFSSource"
                , "--source-ordering-field", this.tabMeta.getSourceOrderingField()
                , "--target-base-path", String.valueOf(this.tabMeta.getDumpDir(this, this.hiveConnMeta))
                , "--target-table", this.tableName + "/" + this.tabMeta.getDataXName()
                , "--props", String.valueOf(fsSourcePropsPath)
                , "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
                , "--enable-sync"
        );

        if (this.tabMeta.getHudiTabType() == HudiWriteTabType.MOR) {
            handle.addAppArgs("--disable-compaction");
        }


        CountDownLatch countDownLatch = new CountDownLatch(1);
        // SparkAppHandle.State[] finalState = new SparkAppHandle.State[1];
        SparkAppHandle sparkAppHandle = handle.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
//                    System.out.println(sparkAppHandle.getAppId());
//                    System.out.println("state:" + sparkAppHandle.getState().toString());
                SparkAppHandle.State state = sparkAppHandle.getState();
                if (state.isFinal()) {
                    // finalState[0] = state;
                    System.out.println("Info:" + sparkAppHandle.getState());
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
            throw new TisException("spark app:" + sparkAppHandle.getAppId() + " execute result not successfule:" + sparkAppHandle.getState());
        }
    }

    private IPath getSourcePropsPath() {
        ITISFileSystem fs = hudiWriter.getFs().getFileSystem();
        return fs.getPath(getDumpDir(), "meta/" + this.tableName + "-source.properties");
    }
}
