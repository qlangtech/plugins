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

package com.qlangtech.tis.plugin.datax.hudi.spark;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.spark.launcher.SparkLauncher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-18 11:41
 **/
public class SparkSubmitParams implements Describable<SparkSubmitParams> {

//        handle.setConf(SparkLauncher.DRIVER_MEMORY, "4G");
//        handle.setConf(SparkLauncher.EXECUTOR_MEMORY, "6G");

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String driverMemory;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String executorMemory;


    public void setHandle(SparkLauncher handle) {
        handle.setConf(SparkLauncher.DRIVER_MEMORY, driverMemory);
        handle.setConf(SparkLauncher.EXECUTOR_MEMORY, executorMemory);
    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<SparkSubmitParams> {

        public boolean validateDriverMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (validateMemory(msgHandler, context, fieldName, value)) {
                return false;
            }
            return true;
        }

        public boolean validateExecutorMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (validateMemory(msgHandler, context, fieldName, value)) {
                return false;
            }
            return true;
        }

        private boolean validateMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                Specification.parse(value).normalizeMemory();
            } catch (Throwable e) {
                msgHandler.addFieldError(context, fieldName, "内容格式有误");
                return true;
            }
            return false;
        }


        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
