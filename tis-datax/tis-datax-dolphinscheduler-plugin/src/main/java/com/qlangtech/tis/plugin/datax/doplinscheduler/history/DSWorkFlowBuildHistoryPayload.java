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

package com.qlangtech.tis.plugin.datax.doplinscheduler.history;

import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;

/**
 * DS 触发的任务执行状态追踪
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-21 15:05
 **/
public class DSWorkFlowBuildHistoryPayload extends WorkFlowBuildHistoryPayload {
    public DSWorkFlowBuildHistoryPayload(Integer tisTaskId, ICommonDAOContext daoContext) {
        super(tisTaskId, daoContext);
    }

    @Override
    public ExecResult processExecHistoryRecord() {
        Long spiWorkflowInstanceId = this.getSPIWorkflowInstanceId();
        return null;
    }

    @Override
    public Class<DSWorkFlowBuildHistoryPayloadFactory> getFactory() {
        return DSWorkFlowBuildHistoryPayloadFactory.class;
    }
}
