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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.config.aliyun.IHttpToken;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 11:59
 **/
public class ESContext implements IDataxContext {
    private final DataXElasticsearchWriter writer;
    private final IHttpToken token;
    private final ESTableAlias mapper;

    public ESContext(DataXElasticsearchWriter writer, ESTableAlias mapper) {
        this.writer = writer;
        this.token = writer.getToken();
        Objects.requireNonNull(this.token, "token can not be null");
        this.mapper = mapper;
    }

    public String getEndpoint() {
        return token.getEndpoint();
    }

    //public boolean isContainUserName() {
//        return StringUtils.isNotEmpty(token.getAccessKeyId());
//    }

    //    public boolean isContainPassword() {
//        return StringUtils.isNotEmpty(token.getAccessKeySecret());
//    }
    // 当用户没有填写认证信息的时候需要有一个占位符，不然提交请求时会报错
    public String getUserName() {
        return StringUtils.defaultIfBlank(token.getAccessKeyId(), "default");
    }

    public String getPassword() {
        return StringUtils.defaultIfBlank(token.getAccessKeySecret(), "******");
    }

    public String getIndex() {
        return this.writer.index;
    }

//    public String getType() {
//        return this.writer.type;
//    }

    public String getColumn() {
        JSONArray cols = this.mapper.getSchemaCols();
        Objects.requireNonNull(cols, "prop cols of mapper can not be null");
        return cols.toJSONString();
    }

    public Boolean getCleanup() {
        return this.writer.cleanup;
    }

    public Integer getBatchSize() {
        return this.writer.batchSize;
    }

    public Integer getTrySize() {
        return this.writer.trySize;
    }

    public Integer getTimeout() {
        return this.writer.timeout;
    }

    public Boolean getDiscovery() {
        return writer.discovery;
    }

    public Boolean getCompression() {
        return writer.compression;
    }

    public Boolean getMultiThread() {
        return writer.multiThread;
    }

    public Boolean getIgnoreWriteError() {
        return writer.ignoreWriteError;
    }

    public Boolean getIgnoreParseError() {
        return writer.ignoreParseError;
    }

    public String getAlias() {
        return writer.alias;
    }

    public String getAliasMode() {
        return writer.aliasMode;
    }

    public String getSettings() {
        return writer.settings;
    }

    public String getSplitter() {
        return writer.splitter;
    }

    public Boolean getDynamic() {
        return writer.dynamic;
    }

    public boolean isContainSettings() {
        return StringUtils.isNotBlank(this.writer.settings);
    }


    public boolean isContainAliasMode() {
        return StringUtils.isNotBlank(this.writer.aliasMode);
    }

    public boolean isContainIndex() {
        return StringUtils.isNotBlank(this.writer.index);
    }

//    public boolean isContainType() {
//        return StringUtils.isNotBlank(this.writer.type);
//    }

    public boolean isContainSplitter() {
        return StringUtils.isNotBlank(this.writer.splitter);
    }

    public boolean isContainTimeout() {
        return this.writer.timeout != null;
    }

    public boolean isContainMultiThread() {
        return this.writer.multiThread != null;
    }

    public boolean isContainEndpoint() {
        return StringUtils.isNotBlank(this.writer.endpoint);
    }

    public boolean isContainCleanup() {
        return this.writer.cleanup != null;
    }

    public boolean isContainDiscovery() {
        return this.writer.discovery != null;
    }

    public boolean isContainTrySize() {
        return this.writer.trySize != null;
    }

    public boolean isContainAlias() {
        return StringUtils.isNotBlank(this.writer.alias);
    }

    public boolean isContainDynamic() {
        return this.writer.dynamic != null;
    }

    public boolean isContainIgnoreParseError() {
        return this.writer.ignoreParseError != null;
    }

    public boolean isContainBatchSize() {
        return this.writer.batchSize != null;
    }

    public boolean isContainCompression() {
        return this.writer.compression != null;
    }

    public boolean isContainIgnoreWriteError() {
        return this.writer.ignoreWriteError != null;
    }

    public static void main(String[] args) {
        //  BeanUtilsBean.getInstance().describe()


    }

}
