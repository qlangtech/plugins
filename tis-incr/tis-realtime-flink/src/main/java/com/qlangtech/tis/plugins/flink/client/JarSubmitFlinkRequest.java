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

package com.qlangtech.tis.plugins.flink.client;

import com.qlangtech.plugins.incr.flink.TISFlinkCDCStart;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugins.flink.client.util.JarArgUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class JarSubmitFlinkRequest {
    private String jobName;
    private List<URL> userClassPaths;
    /**
     * 是否需要cache 下载好的jar包
     */
    // private boolean cache;
    private String dependency;
    private Integer parallelism;
    private String programArgs;
    private String entryClass;
    private String savepointPath;

    /**
     * Flag indicating whether non restored state is allowed if the savepoint contains state for an
     * operator that is not part of the job.
     *
     * @see SavepointRestoreSettings
     */
    private Boolean allowNonRestoredState;

    public static JarSubmitFlinkRequest createFlinkJobRequest(
            TISFlinkCDCStreamFactory factory, TargetResName collection
            , File streamUberJar, Consumer<JarSubmitFlinkRequest> requestSetter) {
        if (!Objects.requireNonNull(streamUberJar, "streamUberJar can not be null").exists()) {
            throw new IllegalArgumentException("streamUberJar path:" + streamUberJar.getAbsolutePath() + " must be exist");
        }
        JarSubmitFlinkRequest request = new JarSubmitFlinkRequest();
        request.setJobName(collection.getName());
        request.setParallelism(factory.parallelism);
        request.setEntryClass(TISFlinkCDCStart.class.getName());


        request.setProgramArgs(collection.getName());
        request.setDependency(streamUberJar.getAbsolutePath());
        requestSetter.accept(request);
        return request;
    }

    public PackagedProgram createFlinkJobProgram() throws ProgramInvocationException {
        // try {
        File jarFile = new File(this.getDependency()); //jarLoader.downLoad(request.getDependency(), request.isCache());
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("file is not exist:" + jarFile.getAbsolutePath());
        }
        List<String> programArgs = JarArgUtil.tokenizeArguments(this.getProgramArgs());

        PackagedProgram.Builder programBuilder = PackagedProgram.newBuilder();
        programBuilder.setEntryPointClassName(this.getEntryClass());
        programBuilder.setJarFile(jarFile);


        if (CollectionUtils.isNotEmpty(this.getUserClassPaths())) {
            programBuilder.setUserClassPaths(this.getUserClassPaths());
        }

        if (programArgs.size() > 0) {
            programBuilder.setArguments(programArgs.toArray(new String[programArgs.size()]));
        }

        final SavepointRestoreSettings savepointSettings;
        String savepointPath = this.getSavepointPath();
        if (StringUtils.isNotEmpty(savepointPath)) {
            Boolean allowNonRestoredOpt = this.getAllowNonRestoredState();
            boolean allowNonRestoredState = allowNonRestoredOpt != null && allowNonRestoredOpt.booleanValue();
            savepointSettings = SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
        } else {
            savepointSettings = SavepointRestoreSettings.none();
        }

        programBuilder.setSavepointRestoreSettings(savepointSettings);
        return (programBuilder.build());
    }

    // private Resource resource;
    public List<URL> getUserClassPaths() {
        return userClassPaths;
    }

    public void setUserClassPaths(List<URL> userClassPaths) {
        this.userClassPaths = userClassPaths;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }


//    public boolean isCache() {
//        return cache;
//    }
//
//    public void setCache(boolean cache) {
//        this.cache = cache;
//    }

    public String getDependency() {
        return dependency;
    }

    public void setDependency(String dependency) {
        this.dependency = dependency;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        if (parallelism == null) {
            throw new IllegalArgumentException("param parallelism can not be null");
        }
        this.parallelism = parallelism;
    }

    public String getProgramArgs() {
        return programArgs;
    }

    public void setProgramArgs(String programArgs) {
        this.programArgs = programArgs;
    }

    public String getEntryClass() {
        return entryClass;
    }

    public void setEntryClass(String entryClass) {
        this.entryClass = entryClass;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    public Boolean getAllowNonRestoredState() {
        return allowNonRestoredState;
    }

    public void setAllowNonRestoredState(Boolean allowNonRestoredState) {
        this.allowNonRestoredState = allowNonRestoredState;
    }


    public void validate() throws Exception {
        Objects.requireNonNull(dependency, "dependency can not be null");
        Objects.requireNonNull(parallelism, "parallelism can not be null");
        Objects.requireNonNull(entryClass, "entryClass can not be null");
    }
}

