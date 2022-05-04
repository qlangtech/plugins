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

package com.qlangtech.tis.hive.impl;

import com.qlangtech.tis.config.hive.IHiveUserToken;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.hive.HiveUserToken;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-03 09:45
 **/
public class DefaultHiveUserToken extends HiveUserToken {

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String userName;

    @FormField(ordinal = 6, type = FormFieldType.PASSWORD, validate = {})
    public String password;

    @Override
    public IHiveUserToken createToken() {
        return new IHiveUserToken(){
            @Override
            public String getUserName() {
                return userName;
            }

            @Override
            public String getPassword() {
                return password;
            }
        };
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<HiveUserToken> {
        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
