/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Maps;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.SinkFuncs;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-18 10:57
 **/
public class TestBasicFlinkSourceHandle extends BasicFlinkSourceHandle implements Serializable {
    final String tabName;

    private StreamTableEnvironment tabEnv;

    private final Map<String, TableResult> sourceTableQueryResult = Maps.newHashMap();

    public TestBasicFlinkSourceHandle(final String tabName) {
        this.tabName = tabName;
    }

    @Override
    protected StreamExecutionEnvironment getFlinkExecutionEnvironment() {
        StreamExecutionEnvironment evn = super.getFlinkExecutionEnvironment();
       // evn.enableCheckpointing(1000);
        return evn;
    }

    @Override
    protected JobExecutionResult executeFlinkJob(TargetResName dataxName, StreamExecutionEnvironment env) throws Exception {
        // return super.executeFlinkJob(dataxName, env);
        // 测试环境下不能执行 return env.execute(dataxName.getName()); 不然单元测试不会自动退出了
        return null;
    }

    @Override
    protected void processTableStream(Map<String, DataStream<DTO>> streamMap, SinkFuncs sinkFunction) {
        sinkFunction.add2Sink(tabName, streamMap.get(tabName));
        if (tabEnv == null) {
            StreamExecutionEnvironment env = sinkFunction.env;
            tabEnv = StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance()
                            .useBlinkPlanner()
                            .inStreamingMode()
                            .build());
        }

        tabEnv.createTemporaryView(tabName, streamMap.get(tabName));
        sourceTableQueryResult.put(tabName, tabEnv.executeSql("SELECT * FROM " + tabName));
    }

    public CloseableIterator<Row> getRowSnapshot(String tabName) {
        TableResult tableResult = sourceTableQueryResult.get(tabName);
        Objects.requireNonNull(tableResult, "tabName:" + tabName + " relevant TableResult can not be null");
        CloseableIterator<Row> collect = tableResult.collect();
        return collect;
    }

    /**
     * 终止flink
     */
    public void cancel() {
        try {
            for (TableResult tabResult : sourceTableQueryResult.values()) {
                tabResult.getJobClient().get().cancel().get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
