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

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql.startup;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.ververica.cdc.connectors.mysql.table.StartupMode;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-29 11:27
 **/
public class TimestampStartupOptions extends StartupOptions {

    @FormField(ordinal = 0, type = FormFieldType.DateTime, validate = {Validator.require})
    public Long startupTimestampMillis;

    @Override
    public com.ververica.cdc.connectors.mysql.table.StartupOptions getOptionsType() {
        return com.ververica.cdc.connectors.mysql.table.StartupOptions.timestamp(startupTimestampMillis);
    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<StartupOptions> {
        @Override
        public String getDisplayName() {
            return StartupMode.TIMESTAMP.name();
        }

        public boolean validateStartupTimestampMillis(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

//            long timestamp = Long.parseLong(value);
//            if (Math.abs(TimeFormat.getCurrentTimeStamp() - timestamp) > 50000000) {
//                msgHandler.addFieldError(context, fieldName, "时间戳设置有误");
//                return false;
//            }

            return true;
        }

    }
}
