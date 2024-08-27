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

package com.qlangtech.tis.datax.executor;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.DataXJobSubmit.InstanceType;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-18 17:57
 **/
public interface ITaskExecutorContext {
    /**
     * DAG 上下游之间传递参数
     *
     * @param key
     * @param value
     */
    void appendData2WfContext(String key, Object value);

    public JSONObject getInstanceParams();

    public JSONObject getJobParams();

    public InstanceType getJobTriggerType();

    /**
     * DAG 工作流实例ID(三方外部DAG系统的WorkflowId)
     *
     * @return
     */
    public Long getWfInstanceId();

    /**
     * 记录日志
     *
     * @param infoMsg
     * @param var
     */
    void infoLog(String infoMsg, Object... var);

    void errorLog(String errorMsg, Exception e);

    /**
     * 最终提交
     */
    default void finallyCommit() {

    }
}
