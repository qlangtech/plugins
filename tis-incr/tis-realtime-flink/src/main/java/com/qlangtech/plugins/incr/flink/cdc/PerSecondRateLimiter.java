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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.cdc.impl.ResettableRateLimitStrategy;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.incr.TISRateLimiter;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 12:22
 **/
public class PerSecondRateLimiter extends BasicRateLimiter {

//    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
//    public Integer recordsPerSecond;

    @Override
    public RateLimiterStrategy getStrategy() {
        return new ResettableRateLimitStrategy(Integer.MAX_VALUE);
    }

    @Override
    public boolean supportRateLimiter() {
        return true;
    }

    @TISExtension
    public static class Desc extends Descriptor<TISRateLimiter> {
        public Desc() {
            super();
        }

        public boolean validateRecordsPerSecond(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            Integer limit = Integer.parseInt(val);
            if (limit < 100) {
                msgHandler.addFieldError(context, fieldName, "必须大于100");
                return false;
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
