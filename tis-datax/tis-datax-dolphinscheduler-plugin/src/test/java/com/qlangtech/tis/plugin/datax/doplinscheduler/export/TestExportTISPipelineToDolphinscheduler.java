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

package com.qlangtech.tis.plugin.datax.doplinscheduler.export;

import com.qlangtech.tis.plugin.datax.doplinscheduler.TestDSWorkflowPayload;
import junit.framework.TestCase;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-02 18:32
 **/
public class TestExportTISPipelineToDolphinscheduler extends TestCase {

    public void testAddProjectParameters() {
        ExportTISPipelineToDolphinscheduler export2DS
                = TestDSWorkflowPayload.createExportTISPipelineToDolphinscheduler();

        export2DS.callback = new DSTISCallback();
        export2DS.callback.tisAddress = "192.168.28.201";
        export2DS.callback.tisHTTPHost = "http://192.168.28.201:8080";
        export2DS.addProjectParameters();
    }
}
