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

package com.qlangtech.plugins.incr.flink.chunjun.poll;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-28 15:42
 **/
public class RunInterval extends Polling {

    public static final String DISPLAY_NAME = "RunInterval";

//        params.put("polling", true);
//        params.put("pollingInterval", 3000);
//        params.put("increColumn", "id");
//        params.put("startLocation", 0);

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String incrColumn;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer pollingInterval;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String startLocation;

    @TISExtension
    public static class DftDesc extends Descriptor<Polling> {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME;
        }

        public boolean validatePollingInterval(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (Integer.parseInt(value) < 1000) {
                msgHandler.addFieldError(context, fieldName, "不能小于1000，1秒");
                return false;
            }
            return true;
        }

    }
}
