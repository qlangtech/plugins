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

package com.qlangtech.tis.hdfs.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IHttpToken;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.AliyunEndpoint;
import com.qlangtech.tis.plugin.aliyun.AccessKey;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/**
 * https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.11/jindofs/hadoop/jindosdk_ide_hadoop.md
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-26 20:12
 **/
public class AliayunJindoFSFactory extends HdfsFileSystemFactory {
    public static final String FIELD_ENDPOINT = "endpoint";
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String endpoint;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String bucket;

    public static String nullHdfsSiteContent() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "</configuration> ";
    }

    @Override
    protected void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        config.set("fs.AbstractFileSystem.oss.impl", com.aliyun.jindodata.oss.OSS.class.getName());
        config.set("fs.oss.impl", com.aliyun.jindodata.oss.JindoOssFileSystem.class.getName());

        AliyunEndpoint end = getAliyunEndpoint();
        AccessKey accessKey = end.getAccessKey();

        config.set("fs.oss.accessKeyId", accessKey.getAccessKeyId());
        config.set("fs.oss.accessKeySecret", accessKey.getAccessKeySecret());
        config.set("fs.oss.endpoint", end.getEndpointHost());
        config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "oss://" + this.bucket + "/");
    }

    protected AliyunEndpoint getAliyunEndpoint() {
        return IHttpToken.getAliyunEndpoint(endpoint);
    }

    @TISExtension
    public static class DefaultDescriptor extends HdfsFileSystemFactory.DefaultDescriptor {
        public DefaultDescriptor() {
            super();
            registerSelectOptions(FIELD_ENDPOINT, () -> ParamsConfig.getItems(IHttpToken.KEY_FIELD_ALIYUN_TOKEN));
        }

        protected boolean isFSDefaultNameKeyInValid(String hdfsAddress) {
            return false;
        }

        @Override
        protected void processError(IControlMsgHandler msgHandler, Context context, Exception e) {
            TisException errMsg = TisException.create(e.getMessage(), e);
            throw errMsg;
        }

        @Override
        public String getDisplayName() {
            return "Aliyun-Jindo-" + super.getDisplayName();
        }
    }
}
