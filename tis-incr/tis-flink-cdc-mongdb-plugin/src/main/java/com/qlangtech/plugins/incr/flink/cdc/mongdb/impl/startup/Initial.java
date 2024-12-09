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

package com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.startup;

import com.qlangtech.plugins.incr.flink.cdc.mongdb.MongoCDCStartupOptions;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource.Builder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 14:13
 **/
public class Initial extends MongoCDCStartupOptions {

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer copyExistingMaxThreads;

    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer copyExistingQueueSize;

    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer pollMaxBatchSize;

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer pollAwaitTimeMillis;

    @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer heartbeatIntervalMillis;


    @Override
    public void appendProperty(Builder<DTO> builder) {
        if (this.copyExistingMaxThreads != null) {
            builder.copyExistingMaxThreads(this.copyExistingMaxThreads);
        }
        if (this.copyExistingQueueSize != null) {
            builder.copyExistingQueueSize(this.copyExistingQueueSize);
        }
        if (this.pollMaxBatchSize != null) {
            builder.pollMaxBatchSize(this.pollMaxBatchSize);
        }
        if (this.pollAwaitTimeMillis != null) {
            builder.pollAwaitTimeMillis(this.pollAwaitTimeMillis);
        }
        if (this.heartbeatIntervalMillis != null) {
            builder.heartbeatIntervalMillis(this.heartbeatIntervalMillis);
        }
    }

    @Override
    public StartupOptions getOptionsType() {
        return option();
    }

    public static StartupOptions option() {
        return StartupOptions.initial();
    }

    @TISExtension
    public static final class DftDescriptor extends Descriptor<MongoCDCStartupOptions> {
        public DftDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return option().startupMode.name();
        }
    }
}
