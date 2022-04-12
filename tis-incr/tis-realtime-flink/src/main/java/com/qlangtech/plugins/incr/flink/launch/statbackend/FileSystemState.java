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

import com.qlangtech.plugins.incr.flink.launch.FlinkDescriptor;
import com.qlangtech.plugins.incr.flink.launch.StateBackendFactory;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-04-11 21:52
 **/
@Public
public class FileSystemState extends StateBackendFactory {

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String checkpointDir;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Integer smallFileThreshold;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Integer writeBufferSize;

    @Override
    public void setProps(StreamExecutionEnvironment env) {

        FsStateBackendFactory factory = new FsStateBackendFactory();

        Configuration config = new Configuration();

        config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, this.checkpointDir);
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD
                , MemorySize.parse(String.valueOf(smallFileThreshold), MemorySize.MemoryUnit.KILO_BYTES));
        config.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, this.writeBufferSize);

        env.setStateBackend(factory.createFromConfig(config, FileSystemState.class.getClassLoader()));
    }


    @TISExtension()
    public static class DefaultDescriptor extends FlinkDescriptor<StateBackendFactory> {

        public DefaultDescriptor() {
            super();
            this.addFieldDescriptor("checkpointDir", CheckpointingOptions.CHECKPOINTS_DIRECTORY);
            this.addFieldDescriptor("smallFileThreshold", CheckpointingOptions.FS_SMALL_FILE_THRESHOLD);
            this.addFieldDescriptor("writeBufferSize", CheckpointingOptions.FS_WRITE_BUFFER_SIZE);
        }


        @Override
        public String getDisplayName() {
            return "FSState";
        }
    }

}
