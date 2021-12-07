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

package com.qlangtech.tis.plugin.aliyun;


import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class AliyunEndpoint extends ParamsConfig implements IAliyunToken {

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.url})
    public String endpoint;

    // 可以为空
    @FormField(ordinal = 2, validate = {})
    public String accessKeyId;

    // 可以为空
    @FormField(ordinal = 3, type = FormFieldType.PASSWORD, validate = {})
    public String accessKeySecret;

    @Override
    public IAliyunToken createConfigInstance() {
        return this;
    }

    @Override
    public String getEndpoint() {
        return this.endpoint;
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @Override
    public String getAccessKeyId() {
        return this.accessKeyId;
    }

    @Override
    public String getAccessKeySecret() {
        return this.accessKeySecret;
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<ParamsConfig> {
//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            ParseDescribable<ParamsConfig> endpoint = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
//            AliyunEndpoint aliyunEndpoint = (AliyunEndpoint)endpoint.instance;
//
//            return true;
//        }

        @Override
        public String getDisplayName() {
            return "Aliyun-Endpoint";
        }
    }
}
