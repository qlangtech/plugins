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

package com.qlangtech.plugins.incr.flink.launch.statbackend;

import com.qlangtech.plugins.incr.flink.launch.StateBackendFactory;
import com.qlangtech.tis.extension.Descriptor;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-24 12:19
 * @see org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory
 * @see org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions
 * @see org.apache.flink.contrib.streaming.state.RocksDBOptions
 **/
public class EmbeddedRocksDBState extends StateBackendFactory {
    @Override
    public void setProps(StreamExecutionEnvironment env) {
        EmbeddedRocksDBStateBackendFactory factory = new EmbeddedRocksDBStateBackendFactory();
        ReadableConfig config = null;
        ClassLoader classLoader = null;
        env.setStateBackend(factory.createFromConfig(config, classLoader));
    }

    // @TISExtension()
    public static class DefaultDescriptor extends Descriptor<StateBackendFactory> {

        public DefaultDescriptor() {
            super();
//            Options<StateBackendFactory> opts = FlinkPropAssist.createOpts(this);
//            opts.addFieldDescriptor("latencyTrackEnable", StateBackendOptions.LATENCY_TRACK_ENABLED);
//            opts.addFieldDescriptor("trackSampleInterval", StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL);
//            opts.addFieldDescriptor("trackHistorySize", StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE);
        }


        @Override
        public String getDisplayName() {
            return "RocksDBState";
        }
    }
}
