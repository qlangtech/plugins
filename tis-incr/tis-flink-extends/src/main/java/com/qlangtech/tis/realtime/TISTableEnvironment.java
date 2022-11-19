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

package com.qlangtech.tis.realtime;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-11-19 16:29
 **/
public class TISTableEnvironment {
    private final StreamTableEnvironment tabEnv;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TISTableEnvironment.class);

    public TISTableEnvironment(StreamTableEnvironment tabEnv) {
        this.tabEnv = tabEnv;
    }

    public TableResult executeSql(String sql) {
        TableResult tabResult = tabEnv.executeSql(sql);
        Optional<JobClient> jobClient = tabResult.getJobClient();
        logger.info("submit flink job: {}", jobClient.get().getJobID());
        return tabResult;
    }

}
