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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.qlangtech.plugins.incr.flink.cdc.oracle.utils.OracleTestUtils;
import com.qlangtech.plugins.incr.flink.slf4j.TISLoggerConsumer;
import org.apache.flink.test.util.AbstractTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;



/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-11 11:24
 **/
public class TestTISFlinkCDCOracleSourceFunction extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTISFlinkCDCOracleSourceFunction.class);

    private OracleContainer oracleContainer =
            OracleTestUtils.ORACLE_CONTAINER.withLogConsumer(new TISLoggerConsumer(LOG));

    public void testBinlogMonitor() throws Exception {
        //   TISFlinkCDCMysqlSourceFunction.main(new String[]{});
    }
}
