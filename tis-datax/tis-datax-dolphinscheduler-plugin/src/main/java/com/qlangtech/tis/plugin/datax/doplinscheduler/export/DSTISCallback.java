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

package com.qlangtech.tis.plugin.datax.doplinscheduler.export;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-03 11:30
 **/
public class DSTISCallback implements Describable<DSTISCallback> {

    @FormField(ordinal = 4, advance = false, type = FormFieldType.INPUTTEXT, validate = {Validator.url, Validator.require})
    public String tisHTTPHost;

    public static String dftTISHTTPHost() {
        return Config.getTISConsoleHttpHost();
    }

    @FormField(ordinal = 5, advance = false, type = FormFieldType.INPUTTEXT, validate = {Validator.hostWithoutPort, Validator.require})
    public String tisAddress;


    public static String dftTISAddress() {
        return Config.getTisHost();
    }

    @TISExtension
    public static class DftDesc extends Descriptor<DSTISCallback> {
        @Override
        public String getDisplayName() {
            return "Default";
        }

        public DftDesc() {
            super();
        }
    }
}
