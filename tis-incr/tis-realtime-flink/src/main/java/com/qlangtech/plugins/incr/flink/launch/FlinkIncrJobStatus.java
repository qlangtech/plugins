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

package com.qlangtech.plugins.incr.flink.launch;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.qlangtech.tis.coredefine.module.action.IFlinkIncrJobStatus;
import com.qlangtech.tis.manage.common.TisUTF8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobID;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 保存当前增量任务的执行状态
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-04-15 12:48
 **/
public class FlinkIncrJobStatus implements IFlinkIncrJobStatus {

    private final File incrJobFile;
    private JobID jobID;
    private List<FlinkSavepoint> savepointPaths = Lists.newArrayList();
    // 当前job的状态
    private State state;

    public boolean containSavepoint(String path) {

        for (FlinkSavepoint sp : savepointPaths) {
            if (StringUtils.equals(path, sp.getPath())) {
                return true;
            }
        }
        return false;
    }

    public void setState(State state) {
        this.state = state;
    }

    public State getState() {
        return this.state;
    }

    public List<FlinkSavepoint> getSavepointPaths() {
        return savepointPaths;
    }

    public FlinkIncrJobStatus(File incrJobFile) {
        this.incrJobFile = incrJobFile;

        if (!incrJobFile.exists()) {
            state = State.NONE;
            return;
        }

        try {
            List<String> lines = FileUtils.readLines(incrJobFile, TisUTF8.get());
            String line = null;
            for (int i = (lines.size() - 1); i >= 0; i--) {
                line = lines.get(i);
                if (StringUtils.indexOf(line, KEY_SAVEPOINT_DIR_PREFIX) > -1) {
                    savepointPaths.add(DftFlinkSavepoint.deSerialize(line));
                    if (state == null) {
                        state = State.STOPED;
                    }
                } else if (jobID == null) {
                    // jobId 一定是最后一个读到的
                    jobID = JobID.fromHexString(line);
                    if (state == null) {
                        state = State.RUNNING;
                    }
                }
            }
            if (state == null) {
                // 说明是空文件
                state = State.NONE;
                // throw new IllegalStateException("job state can not be null");
            }
            // Collections.reverse(savepointPaths);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createNewJob(JobID jobID) {
        try {
            FileUtils.writeLines(incrJobFile
                    , Lists.newArrayList("#" + this.getClass().getName(),
                            jobID.toHexString()), false);
            this.savepointPaths = Lists.newArrayList();
            this.state = State.RUNNING;
            this.jobID = jobID;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @JSONField(serialize = false)
    public JobID getLaunchJobID() {
        return this.jobID;
    }

    public void stop(String savepointDirectory) {
        try {
            DftFlinkSavepoint savepoint = new DftFlinkSavepoint(savepointDirectory);
            //StringBuffer spInfo = new StringBuffer();
            //spInfo.append(savepointDirectory).append(";").append(System.currentTimeMillis());
            FileUtils.writeLines(incrJobFile, Collections.singletonList(savepoint.serialize()), true);
            this.savepointPaths.add(savepoint);
            this.state = State.STOPED;
        } catch (IOException e) {
            throw new RuntimeException("savepointDirectory:" + savepointDirectory, e);
        }
    }

    public void relaunch(JobID jobID) {
        try {
            FileUtils.writeLines(incrJobFile, Collections.singletonList(jobID.toHexString()), true);
            this.state = State.RUNNING;
            this.jobID = jobID;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void cancel() {
        try {
            FileUtils.forceDelete(incrJobFile);
            this.state = State.NONE;
            this.jobID = null;
            this.savepointPaths = Lists.newArrayList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private static class DftFlinkSavepoint extends FlinkSavepoint {
        private final static String KEY_TUPLE_SPLIT = ";";

        public DftFlinkSavepoint(String path) {
            super(path, System.currentTimeMillis());
        }

        public String serialize() {
            StringBuffer spInfo = new StringBuffer();
            spInfo.append(this.getPath()).append(KEY_TUPLE_SPLIT).append(this.getCreateTimestamp());
            return String.valueOf(spInfo);
        }

        public static FlinkSavepoint deSerialize(String seri) {
            String[] tuple = StringUtils.split(seri, KEY_TUPLE_SPLIT);
            if (tuple.length != 2) {
                throw new IllegalStateException("param is not illegal:" + seri);
            }
            return new FlinkSavepoint(tuple[0], Long.parseLong(tuple[1]));
        }
    }

}
