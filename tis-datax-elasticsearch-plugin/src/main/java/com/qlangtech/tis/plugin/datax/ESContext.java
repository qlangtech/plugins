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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxContext;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 11:59
 **/
public class ESContext implements IDataxContext {
    private final DataXElasticsearchWriter writer;
    private final IAliyunToken token;

    public ESContext(DataXElasticsearchWriter writer) {
        this.writer = writer;
        this.token = writer.getToken();
        Objects.requireNonNull(this.token, "token can not be null");
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

    public String getType() {
        return this.writer.type;
    }

//    public String getColumn() {
//        return this.writer.column;
//    }

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

    public boolean isContainType() {
        return StringUtils.isNotBlank(this.writer.type);
    }

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
