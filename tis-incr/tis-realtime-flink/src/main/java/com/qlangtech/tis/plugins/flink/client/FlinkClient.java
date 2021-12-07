/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugins.flink.client;

import com.qlangtech.tis.plugins.flink.client.util.JarArgUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-10 17:47
 **/
public class FlinkClient {

    private static final Logger logger = LoggerFactory.getLogger(FlinkClient.class);
    // private JarLoader jarLoader;

//    public void setJarLoader(JarLoader jarLoader) {
//        this.jarLoader = jarLoader;
//    }

    public JobID submitJar(ClusterClient clusterClient, JarSubmitFlinkRequest request) throws Exception {
        logger.trace("start submit jar request,entryClass:{}", request.getEntryClass());
        // try {
        File jarFile = new File(request.getDependency()); //jarLoader.downLoad(request.getDependency(), request.isCache());
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("file is not exist:" + jarFile.getAbsolutePath());
        }
        List<String> programArgs = JarArgUtil.tokenizeArguments(request.getProgramArgs());

        PackagedProgram.Builder programBuilder = PackagedProgram.newBuilder();
        programBuilder.setEntryPointClassName(request.getEntryClass());
        programBuilder.setJarFile(jarFile);


        if (CollectionUtils.isNotEmpty(request.getUserClassPaths())) {
            programBuilder.setUserClassPaths(request.getUserClassPaths());
        }

        if (programArgs.size() > 0) {
            programBuilder.setArguments(programArgs.toArray(new String[programArgs.size()]));
        }

        final SavepointRestoreSettings savepointSettings;
        String savepointPath = request.getSavepointPath();
        if (StringUtils.isNotEmpty(savepointPath)) {
            Boolean allowNonRestoredOpt = request.getAllowNonRestoredState();
            boolean allowNonRestoredState = allowNonRestoredOpt != null && allowNonRestoredOpt.booleanValue();
            savepointSettings = SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
        } else {
            savepointSettings = SavepointRestoreSettings.none();
        }

        programBuilder.setSavepointRestoreSettings(savepointSettings);
        //programBuilder.setSavepointRestoreSettings();
        PackagedProgram program = programBuilder.build();
//            PackagedProgram program = new PackagedProgram(jarFile, request.getEntryClass(),
//                    programArgs.toArray(new String[programArgs.size()]));
        // final ClassLoader classLoader = program.getUserCodeClassLoader();
        //  Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new Configuration());
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, new Configuration(), request.getParallelism(), false);
        //FlinkPlan plan = ClusterClient.getOptimizedPlan(optimizer, program, request.getParallelism());
        // Savepoint restore settings
        // set up the execution environment
        // List<URL> jarFiles = FileUtil.createPath(jarFile);
        // CompletableFuture.supplyAsync(() -> {
        try {

            CompletableFuture<JobID> submissionResult = clusterClient.submitJob(jobGraph);
            JobID jobId = submissionResult.get();
//                    JobSubmissionResult submissionResult
//                            = clusterClient.run(plan, jarFiles, Collections.emptyList(), classLoader, savepointSettings);
            //logger.trace(" submit jar request sucess,jobId:{}", submissionResult.get());
            //consumer.accept(new SubmitFlinkResponse(true, String.valueOf(jobId)));
            return jobId;
        } catch (Exception e) {
           // String term = e.getMessage() == null ? "." : (": " + e.getMessage());
            logger.error(" submit sql request fail", e);
            // return new SubmitFlinkResponse(term);
            // consumer.accept(new SubmitFlinkResponse(term));
            throw new RuntimeException(e);
        }
        //}).thenAccept(consumer::accept);

        //  } catch (Throwable e) {
//            String term = e.getMessage() == null ? "." : (": " + e.getMessage());
//            logger.error(" submit jar request fail", e);
        //    throw new RuntimeException(e);
        //}
    }

}
