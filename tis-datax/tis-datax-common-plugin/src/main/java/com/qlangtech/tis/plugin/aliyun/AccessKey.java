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

package com.qlangtech.tis.plugin.aliyun;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.AuthToken;
import com.qlangtech.tis.plugin.AuthToken.IAliyunAccessKey;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-31 09:52
 **/
public class AccessKey extends AuthToken implements IAliyunAccessKey {

    // 可以为空
    @FormField(ordinal = 2, validate = {Validator.require, Validator.identity})
    public String accessKeyId;
    // 可以为空
    @FormField(ordinal = 3, type = FormFieldType.PASSWORD, validate = {Validator.require})
    public String accessKeySecret;

    public AccessKey() {
    }

    public AccessKey(String accessKeyId, String accessKeySecret) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }

    public String getAccessKeyId() {
        return this.accessKeyId;
    }

    public String getAccessKeySecret() {
        return this.accessKeySecret;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<AuthToken> {
        @Override
        public String getDisplayName() {
            return KEY_ACCESS;
        }
    }
}
