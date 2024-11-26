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

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.ConfigFileContext.Header;
import com.qlangtech.tis.manage.common.ConfigFileContext.StreamErrorProcess;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder.DolphinSchedulerResponse;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 08:57
 **/
public class DolphinSchedulerEndpoint extends ParamsConfig {
    public static final String DISPLAY_NAME = "ds-endpoint";
    public static final String FIELD_KEY_SERVER_TOKEN = "serverToken";
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    /**
     * example: http://192.168.28.201:12345/dolphinscheduler
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.url})
    public String serverPath;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String serverToken;

    public DolphinSchedulerURLBuilder createSchedulerURLBuilder() {
        return new DolphinSchedulerURLBuilder(this);
    }

    public List<Header> appendToken(List<Header> originHeaders) {
        List<Header> headers = Lists.newArrayList(originHeaders);
        headers.add(new Header("token", this.serverToken));
        headers.add(new Header("Accept", "application/json"));
        return headers;
    }

    @Override
    public DolphinSchedulerEndpoint createConfigInstance() {
        return this;
    }

    private void tryConnect(IControlMsgHandler msgHandler, Context context) {
        //http://192.168.28.201:12345/dolphinscheduler/swagger-ui/index.html?language=zh_CN&lang=cn#/Worker%E5%88%86%E7%BB%84%E7%AE%A1%E7%90%86/queryAllWorkerGroupsPaging
        DolphinSchedulerResponse response = createSchedulerURLBuilder().appendSubPath("worker-groups")
                // .appendQueryParam(FIELD_PAGE_NO, 1).appendQueryParam(FIELD_PAGE_SIZE, 100)
                .appendPageParam()
                .applyGet(Optional.of(new StreamErrorProcess() {
                    @Override
                    public void error(int status, InputStream errstream, IOException e) throws Exception {
                        if (HttpURLConnection.HTTP_UNAUTHORIZED == status) {
                            msgHandler.addFieldError(context, FIELD_KEY_SERVER_TOKEN, "请检查令牌是否有误");
                        } else {
                            super.error(status, errstream, e);
                        }
                    }
                }));
        if (context.hasErrors()) {
            return;
        }
        if (!response.isSuccess()) {
            throw TisException.create(response.getMessage());
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicParamsConfigDescriptor implements IEndTypeGetter {
        public DefaultDescriptor() {
            super(DISPLAY_NAME);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            DolphinSchedulerEndpoint endpoint = postFormVals.newInstance();
            endpoint.tryConnect(msgHandler, context);
            return true;
        }

        @Override
        public String getDisplayName() {
            return DISPLAY_NAME;
        }

        @Override
        public EndType getEndType() {
            return EndType.Dolphinscheduler;
        }
    }
}
