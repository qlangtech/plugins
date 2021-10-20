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

package com.qlangtech.tis.plugins.flink.client;

import java.net.URL;
import java.util.List;
import java.util.Objects;

public class JarSubmitFlinkRequest {
    private String jobName;
    private List<URL> userClassPaths;
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


    /**
     * 是否需要cache 下载好的jar包
     */
    // private boolean cache;

    private String dependency;

    private Integer parallelism;

    private String programArgs;

    private String entryClass;

    private String savepointPath;

    private Boolean allowNonRestoredState;

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

