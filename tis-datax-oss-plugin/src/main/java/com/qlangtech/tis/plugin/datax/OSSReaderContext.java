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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxReaderContext;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-13 18:27
 **/
public class OSSReaderContext implements IDataxReaderContext {
    private final DataXOssReader reader;

    @Override
    public String getTaskName() {
        //throw new UnsupportedOperationException();
        return StringUtils.replace(StringUtils.remove(reader.object, "*"), "/", "-");
    }

    @Override
    public String getSourceEntityName() {
        throw new UnsupportedOperationException();
    }

    public OSSReaderContext(DataXOssReader reader) {
        this.reader = reader;
    }

    public IAliyunToken getOss() {
        return reader.getOSSConfig();
    }

    public String getObject() {
        return reader.object;
    }

    public String getBucket() {
        return reader.bucket;
    }

    public String getCols() {
        return reader.column;
    }

    public String getFieldDelimiter() {
        return reader.fieldDelimiter;
    }

    public String getCompress() {
        return reader.compress;
    }

    public boolean isContainCompress() {
        return StringUtils.isNotBlank(reader.compress);
    }

    public boolean isContainEncoding() {
        return StringUtils.isNotBlank(reader.encoding);
    }

    public boolean isContainNullFormat() {
        return StringUtils.isNotBlank(reader.nullFormat);
    }

    public String getEncoding() {
        return reader.encoding;
    }

    public String getNullFormat() {
        // \N  -> \\N
        return StringEscapeUtils.escapeJava(reader.nullFormat);
    }

    public Boolean getSkipHeader() {
        return reader.skipHeader;
    }

    public boolean isContainCsvReaderConfig() {
        try {
            JSONObject o = JSON.parseObject(reader.csvReaderConfig);
            return o.keySet().size() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public String getCsvReaderConfig() {
        // 为一个json格式
        return reader.csvReaderConfig;
    }


}
