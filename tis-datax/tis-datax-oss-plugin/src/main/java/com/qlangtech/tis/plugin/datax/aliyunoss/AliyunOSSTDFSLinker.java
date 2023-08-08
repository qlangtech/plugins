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

package com.qlangtech.tis.plugin.datax.aliyunoss;

import com.alibaba.citrus.turbine.Context;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.Bucket;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IHttpToken;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.AliyunEndpoint;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.aliyun.AccessKey;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 13:30
 **/
public class AliyunOSSTDFSLinker extends TDFSLinker {
    public static final String DATAX_NAME = "AlyiunOSS";
    public static final String FIELD_BUCKET = "bucket";
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String bucket;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String object;

    private AliyunEndpoint getOSSConfig() {
        return IHttpToken.getAliyunEndpoint(this.linker);
    }

    public OSS createOSSClient() {
        final AliyunEndpoint end = getOSSConfig();
        AccessKey accessKey = end.getAccessKey();
        // OSS ossClient = end.accept(new AuthToken.Visitor<OSS>() {
//            @Override
//            public OSS visit(AccessKey accessKey) {
//
//            }
//        });
//        return ossClient;
        return new OSSClientBuilder().build(end.getEndpoint(), accessKey.getAccessKeyId(), accessKey.getAccessKeySecret());
    }

    @Override
    public ITDFSSession createTdfsSession(Integer timeout) {
        return this.createTdfsSession();
    }

    @Override
    public ITDFSSession createTdfsSession() {
        return new OSSSession(this);
    }

    @Override
    public <T> T useTdfsSession(TDFSSessionVisitor<T> tdfsSession) {
        try {
            return tdfsSession.accept(createTdfsSession());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        protected List<? extends IdentityName> createRefLinkers() {
            return ParamsConfig.getItems(IHttpToken.KEY_FIELD_ALIYUN_TOKEN);
        }

        public boolean validateLinker(IFieldErrorHandler msgHandler, Context context, String fieldName, String endpoint) {
//            AliyunEndpoint end = IHttpToken.getAliyunEndpoint(endpoint);
//            return end.accept(new AuthToken.Visitor<Boolean>() {
//                @Override
//                public Boolean visit(NoneToken noneToken) {
//                    // Validator.require.validate(msgHandler, context, fieldName, null);
//                    msgHandler.addFieldError(context, fieldName, "请填写AccessKey/AccessToken");
//                    return false;
//                }
//
//                @Override
//                public Boolean visit(AccessKey accessKey) {
//                    return true;
//                }
//
//                @Override
//                public Boolean visit(UsernamePassword accessKey) {
//                    msgHandler.addFieldError(context, fieldName, "不支持使用用户名/密码认证方式");
//                    return false;
//                }
//            });
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            AliyunOSSTDFSLinker osstdfsLinker = (AliyunOSSTDFSLinker) postFormVals.newInstance(this, msgHandler);
            return verifyFormOSSRelative(msgHandler, context, osstdfsLinker);
        }

        private static boolean verifyFormOSSRelative(IControlMsgHandler msgHandler, Context context, AliyunOSSTDFSLinker osstdfsLinker) {
            String bucket = osstdfsLinker.bucket;
            //  HttpEndpoint end = osstdfsLinker.getOSSConfig();
            try {
                OSS ossClient = osstdfsLinker.createOSSClient();
//                end.accept(new AuthToken.Visitor<OSS>() {
//                    @Override
//                    public OSS visit(AccessKey accessKey) {
//                        return new OSSClientBuilder().build(end.getEndpoint(), accessKey.getAccessKeyId(), accessKey.getAccessKeySecret());
//                    }
//                });

                List<Bucket> buckets = ossClient.listBuckets();
//                if (buckets.size() < 1) {
//                    msgHandler.addErrorMessage(context, "buckets不能为空");
//                    return false;
//                }
                Optional<Bucket> bucketFind = buckets.stream().filter((b) -> StringUtils.equals(bucket, b.getName())).findFirst();
                if (!bucketFind.isPresent()) {
                    //  msgHandler.addErrorMessage(context, );
                    msgHandler.addFieldError(context, FIELD_BUCKET, "还未创建bucket:" + bucket);
                    return false;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

}
