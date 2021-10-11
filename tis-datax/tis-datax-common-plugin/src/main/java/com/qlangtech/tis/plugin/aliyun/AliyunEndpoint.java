/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
