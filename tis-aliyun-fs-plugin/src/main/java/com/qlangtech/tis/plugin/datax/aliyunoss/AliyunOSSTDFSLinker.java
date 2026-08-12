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
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.BucketInfo;
import com.google.common.collect.Lists;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunAccessKey;
import com.qlangtech.tis.config.aliyun.IAliyunEndpoint;
import com.qlangtech.tis.config.aliyun.IHttpToken;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.GroovyShellUtil;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.action.IParamGetter;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 13:30
 **/
public class AliyunOSSTDFSLinker extends TDFSLinker {
    public static final String DATAX_NAME = "AlyiunOSS";
    public static final String FIELD_BUCKET = "bucket";
    private static final Logger logger = LoggerFactory.getLogger(AliyunOSSTDFSLinker.class);
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String bucket;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String object;


    @Override
    public String getRootPath() {
        return StringUtils.removeStart(this.path, "/");
    }

    protected IAliyunEndpoint getOSSConfig() {
        return IHttpToken.getAliyunEndpoint(this.linker);
    }

    public final OSS createOSSClient() {
        final IAliyunEndpoint end = getOSSConfig();
        IAliyunAccessKey accessKey = end.getAccessKey();
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

    /**
     * 通过 OntologyProperty 的type 获取 valueType的下拉可选项目
     *
     * @return
     */
    public static List<OptionWithEndType> availableBuckets() {
        Map<Class<? extends Descriptor>, Describable> classDescribableMap =
                Objects.requireNonNull(GroovyShellUtil.pluginThreadLocal.get(), "classDescribableMap can not be null");
        //  IPluginContext pluginContext = IPluginContext.getThreadLocalInstance();
//        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(pluginContext.getContext());
//        if (meta.getDelegate().isCreateProcess()) {
//            return Collections.emptyList();
//        }
        if (MapUtils.isEmpty(classDescribableMap)) {
            return Collections.emptyList();
        }
        for (Map.Entry<Class<? extends Descriptor>, Describable> entry : classDescribableMap.entrySet()) {
            // DataXDFSWriter
            if (!(entry.getValue() instanceof TDFSLinker.TDFSLinkerGetter linker)) {
                throw new IllegalStateException("entry.getValue() must be type of "
                        + TDFSLinker.TDFSLinkerGetter.class.getName() + " but now is " + entry.getValue().getClass().getName());
            }
            return getBuckets((AliyunOSSTDFSLinker) linker.getTDFSLinker());
        }
        throw new IllegalStateException("classDescribableMap.entrySet() can not be empty");
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {
        public DftDescriptor() {
            super();
            this.valueChangePipe(KEY_FTP_SERVER_LINK, FIELD_BUCKET)
                    .render(new BiFunction<UploadPluginMeta, IParamGetter, List<? extends Option>>() {
                        @Override
                        public List<? extends Option> apply(UploadPluginMeta pluginMeta, IParamGetter param) {

                            AliyunOSSTDFSLinker linker = new AliyunOSSTDFSLinker();
                            linker.linker = param.getString(KEY_FTP_SERVER_LINK);
                            return getBuckets(linker);
                        }
                    });
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        public String shortComment() {
            return "阿里云对象存储";
        }


        @Override
        protected List<? extends IdentityName> createRefLinkers() {
            return ParamsConfig.getItems(IHttpToken.KEY_FIELD_ALIYUN_TOKEN);
        }

        public boolean validateLinker(IFieldErrorHandler msgHandler, Context context, String fieldName, String endpoint) {
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            AliyunOSSTDFSLinker osstdfsLinker = (AliyunOSSTDFSLinker) postFormVals.newInstance();
            return verifyFormOSSRelative(msgHandler, context, osstdfsLinker);
        }

        private static boolean verifyFormOSSRelative(IControlMsgHandler msgHandler, Context context, AliyunOSSTDFSLinker osstdfsLinker) {
            String bucket = osstdfsLinker.bucket;
            try {
                OSS ossClient = osstdfsLinker.createOSSClient();

                BucketInfo bucketInfo = null;
                try {
                    bucketInfo = ossClient.getBucketInfo(bucket);
                } catch (OSSException e) {
                    logger.error("request bucket info:" + bucket, e);
                    msgHandler.addFieldError(context, FIELD_BUCKET, e.getMessage());
                    return false;
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

    private static List<OptionWithEndType> getBuckets(AliyunOSSTDFSLinker linker) {
        List<OptionWithEndType> buckets = Lists.newArrayList();
        OSS oss = linker.createOSSClient();
        for (Bucket bucket : oss.listBuckets()) {
            buckets.add(new OptionWithEndType(bucket.getName(), bucket.getName(), IEndTypeGetter.EndType.Bucket)
                    .setDescription("create:" + TimeFormat.yyyy_MM_dd.format(bucket.getCreationDate())));
        }
        return buckets;
    }

}
