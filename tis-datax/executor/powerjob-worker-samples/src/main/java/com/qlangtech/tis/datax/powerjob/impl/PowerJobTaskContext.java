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

package com.qlangtech.tis.datax.powerjob.impl;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.DataXJobSubmit.InstanceType;
import com.qlangtech.tis.datax.executor.ITaskExecutorContext;
import com.qlangtech.tis.datax.powerjob.ExecPhase;
import com.qlangtech.tis.datax.powerjob.SplitTabSync;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.MapProcessor;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-18 18:02
 **/
public class PowerJobTaskContext implements ITaskExecutorContext {
    private final TaskContext context;

    public PowerJobTaskContext(TaskContext context) {
        this.context = context;
    }

    @Override
    public InstanceType getJobTriggerType() {
        return InstanceType.DISTRIBUTE;
    }

    public static ExecPhase parse(MapProcessor processor, TaskContext context) {


        if (processor.isRootTask()) {
            return ExecPhase.Prepare;
        }

        Object subTask = context.getSubTask();
        if (subTask != null && subTask instanceof SplitTabSync) {
            return ExecPhase.Mapper;
        }

        throw new IllegalStateException("illegal status");
    }

    @Override
    public void appendData2WfContext(String key, Object value) {
        context.getWorkflowContext().appendData2WfContext(key, value);
    }

    @Override
    public void infoLog(String msg, Object... param) {
        context.getOmsLogger().info(msg, param);
    }

    @Override
    public void errorLog(String msg, Exception e) {
        context.getOmsLogger().error(msg, e);
    }

    @Override
    public JSONObject getJobParams() {
        return JSONObject.parseObject(context.getJobParams());
    }

    @Override
    public Long getWfInstanceId() {
        return context.getWorkflowContext().getWfInstanceId();
    }

    @Override
    public JSONObject getInstanceParams() {
        return JSONObject.parseObject(context.getInstanceParams());
    }


}
