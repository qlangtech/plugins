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

package com.qlangtech.plugins.incr.flink.launch.ckpt;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.launch.CheckpointFactory;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-01 16:22
 **/
public class CKOn extends CheckpointFactory {

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer ckpointInterval;

    @Override
    public void setProps(StreamExecutionEnvironment env) {
        env.enableCheckpointing(this.ckpointInterval);
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<CheckpointFactory> {
        @Override
        public String getDisplayName() {
            return "On";
        }

        private static final int MIN_INTERVAL = 200;

        public boolean validateCkpointInterval(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int interval = 0;
            try {
                interval = Integer.parseInt(val);
            } catch (Throwable e) {

            }
            if (interval < MIN_INTERVAL) {
                msgHandler.addFieldError(context, fieldName, "不能小于最小值：" + MIN_INTERVAL);
                return false;
            }
            return true;

        }
    }
}
