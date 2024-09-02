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

package com.qlangtech.tis.plugin.datax.buildhistory;

import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayloadFactory;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-31 15:20
 **/
public class TestBuildHistoryPayload extends WorkFlowBuildHistoryPayload {
    public TestBuildHistoryPayload(IDataxProcessor dataxProcessor, Integer tisTaskId, ICommonDAOContext daoContext) {
        super(dataxProcessor, tisTaskId, daoContext);
    }

    @Override
    public ExecResult processExecHistoryRecord() {
        return null;
    }

    @Override
    public Class<TestBuildHistoryPayloadFactory> getFactory() {
        return TestBuildHistoryPayloadFactory.class;
    }

}
