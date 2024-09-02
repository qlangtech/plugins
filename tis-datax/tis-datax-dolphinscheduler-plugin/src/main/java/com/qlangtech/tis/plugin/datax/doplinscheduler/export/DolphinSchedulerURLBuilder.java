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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.manage.common.ConfigFileContext.HTTPMethod;
import com.qlangtech.tis.manage.common.ConfigFileContext.Header;
import com.qlangtech.tis.manage.common.ConfigFileContext.StreamErrorProcess;
import com.qlangtech.tis.manage.common.ConfigFileContext.StreamProcess;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.manage.common.HttpUtils.PostParam;
import com.qlangtech.tis.manage.common.PostFormStreamProcess;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.IWorkflowNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-24 10:24
 **/
public class DolphinSchedulerURLBuilder {
    private static final Logger logger = LoggerFactory.getLogger(DolphinSchedulerURLBuilder.class);
    private static final String SLASH = "/";
    private final DolphinSchedulerEndpoint endpoint;
    private final StringBuffer url;
    private final List<PostParam> queryParams = Lists.newArrayList();

    public DolphinSchedulerURLBuilder(DolphinSchedulerEndpoint endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint);
        boolean endWithSlash = StringUtils.endsWith(this.endpoint.serverPath, SLASH);
        if (StringUtils.isEmpty(this.endpoint.serverPath)) {
            throw new IllegalArgumentException("endpoint.serverPath can not be empty");
        }
        this.url = new StringBuffer(endWithSlash ? StringUtils.removeEnd(this.endpoint.serverPath, SLASH) : this.endpoint.serverPath);

    }

    public DolphinSchedulerURLBuilder appendSubPath(Object... subpath) {
        int index = 0;
        for (Object path : subpath) {
            url.append(SLASH).append(Objects.requireNonNull(path, "index " + (index++) + " of subpath can not be null"));
        }
        return this;
    }

    public DolphinSchedulerURLBuilder appendQueryParam(String key, Object value) {
        if (StringUtils.isEmpty(key)) {
            throw new IllegalArgumentException("param key can not be empty");
        }
        this.queryParams.add(new PostParam(key, Objects.requireNonNull(value, "value can not be null")));
        return this;
    }

    private URL build() {
        try {
            if (CollectionUtils.isNotEmpty(this.queryParams)) {
                this.url.append("?").append(this.queryParams.stream()
                        .map((param) -> param.getKey() + "=" + param.getValue()).collect(Collectors.joining("&")));
            }
            // logger.info("apply request URL:{}", this.url.toString());
            return new URL(this.url.toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public DolphinSchedulerResponse applyGet() {
        return applyGet(Optional.empty());
    }

    public DolphinSchedulerResponse applyGet(Optional<StreamErrorProcess> streamErrorProcess) {
        final URL applyURL = this.build();
        return HttpUtils.get(applyURL, new StreamProcess<DolphinSchedulerResponse>() {
            @Override
            public List<Header> getHeaders() {
                return endpoint.appendToken(super.getHeaders());
            }

            @Override
            public void error(int status, InputStream errstream, IOException e) throws Exception {
                //streamProcess.error(status, errstream, e);
                if (streamErrorProcess.isPresent()) {
                    streamErrorProcess.get().error(status, errstream, e);
                } else {
                    super.error(status, errstream, e);
                }
            }

            @Override
            public DolphinSchedulerResponse p(int status
                    , InputStream stream, Map<String, List<String>> headerFields) throws IOException {
                return parseDolphinSchedulerResponse(applyURL, stream);
//                if (!) {
//                    msgHandler.addFieldError(context, FIELD_PROCESS_NAME, result.getString("msg"));
//                    return null;
//                }
//
//                return streamProcess.p(status, stream, headerFields);
            }
        });
    }

    private static DolphinSchedulerResponse parseDolphinSchedulerResponse(URL applyURL, InputStream stream) throws IOException {
        JSONObject result = JSONObject.parseObject(IOUtils.toString(stream, TisUTF8.get()));
        boolean success = result.getBooleanValue("success");
        int code = result.getIntValue("code");
        JSONObject data = result.getJSONObject("data");
        String msg = result.getString("msg");

        return new DolphinSchedulerResponse(applyURL, code, msg, data, success);
    }

    public DolphinSchedulerResponse applyPut(List<PostParam> params, Optional<StreamErrorProcess> streamErrorProcess) {
        final URL applyUrl = this.build();
        return applyRequest(params, streamErrorProcess, applyUrl, HTTPMethod.PUT);
    }

    public DolphinSchedulerResponse applyPost(List<PostParam> params, Optional<StreamErrorProcess> streamErrorProcess) {
        final URL applyUrl = this.build();
        HTTPMethod httpMethod = HTTPMethod.POST;
        return applyRequest(params, streamErrorProcess, applyUrl, httpMethod);
    }

    private DolphinSchedulerResponse applyRequest(List<PostParam> params
            , Optional<StreamErrorProcess> streamErrorProcess, URL applyUrl, HTTPMethod httpMethod) {
        return HttpUtils.process(applyUrl, params, new PostFormStreamProcess<DolphinSchedulerResponse>() {
            @Override
            public List<Header> getHeaders() {
                return endpoint.appendToken(super.getHeaders());
            }

            @Override
            public void error(int status, InputStream errstream, IOException e) throws Exception {
                // streamProcess.error(status, errstream, e);
                if (streamErrorProcess.isPresent()) {
                    streamErrorProcess.get().error(status, errstream, e);
                } else {
                    super.error(status, errstream, e);
                }
            }

            @Override
            public DolphinSchedulerResponse p(int status, InputStream stream, Map<String, List<String>> headerFields) throws IOException {
                return parseDolphinSchedulerResponse(applyUrl, stream);
                //  return streamProcess.p(status, stream, headerFields);
            }
        }, httpMethod);
    }

    public static class DolphinSchedulerResponse {
        private final int code;
        private final String message;
        private final JSONObject data;
        private final boolean success;
        private final URL applyURL;

        public DolphinSchedulerResponse(URL applyURL, int code, String message, JSONObject data, boolean success) {
            this.code = code;
            this.message = message;
            this.data = data;
            this.success = success;
            this.applyURL = applyURL;
        }

        public String errorDescribe() {
            return " error message:"
                    + this.getMessage() + ",code:" + this.getCode() + ",apply url:" + this.getApplyURL();
        }

        public URL getApplyURL() {
            return this.applyURL;
        }

        public int getCode() {
            return this.code;
        }

        public String getMessage() {
            return this.message;
        }

        public JSONObject getData() {
            return this.data;
        }

        public boolean isSuccess() {
            return this.success;
        }
    }

}
