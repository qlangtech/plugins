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

package com.qlangtech.tis.plugin.datax.common;

import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.ds.ContextParamConfig;
import com.qlangtech.tis.plugin.ds.ContextParamConfig.ContextParamValGetter;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.RdbmsRunningContext;
import com.qlangtech.tis.plugin.ds.RunningContext;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-10 23:50
 **/
public class ContextParams {


    public static Map<String, ContextParamConfig> defaultContextParams() {
        ContextParamConfig dbName = new ContextParamConfig("dbName") {
            @Override
            public ContextParamValGetter<RunningContext> valGetter() {
                return new DbNameContextParamValGetter();
            }

            @Override
            public DataType getDataType() {
                return DataType.createVarChar(50);
            }
        };

        ContextParamConfig sysTimestamp = new ContextParamConfig("timestamp") {
            @Override
            public ContextParamValGetter<RunningContext> valGetter() {
                return new SystemTimeStampContextParamValGetter();
            }

            @Override
            public DataType getDataType() {
                return DataType.getType(JDBCTypes.TIMESTAMP);
            }
        };

        ContextParamConfig tableName = new ContextParamConfig("tableName") {
            @Override
            public ContextParamValGetter<RunningContext> valGetter() {
                return new TableNameContextParamValGetter();
            }

            @Override
            public DataType getDataType() {
                return DataType.createVarChar(50);
            }
        };

        return Lists.newArrayList(dbName, tableName, sysTimestamp)
                .stream().collect(Collectors.toMap((cfg) -> cfg.getKeyName(), (cfg) -> cfg));
    }


    public static class DbNameContextParamValGetter implements ContextParamValGetter<RunningContext> {
        @Override
        public Object apply(RunningContext runningContext) {
            return runningContext.getDbName();
        }
    }

    public static class SystemTimeStampContextParamValGetter implements ContextParamValGetter<RunningContext> {
        @Override
        public Object apply(RunningContext runningContext) {
            return System.currentTimeMillis();
        }
    }

    public static class TableNameContextParamValGetter implements ContextParamValGetter<RunningContext> {
        @Override
        public Object apply(RunningContext runningContext) {
            return runningContext.getTable();
        }
    }

}
