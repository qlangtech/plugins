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
import com.qlangtech.tis.datax.IDataxReaderContext;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-13 18:27
 **/
public class OSSReaderContext implements IDataxReaderContext {
    private final DataXOssReader reader;

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

    public String getEncoding() {
        return reader.encoding;
    }

    public String getNullFormat() {
        return reader.nullFormat;
    }

    public Boolean getSkipHeader() {
        return reader.skipHeader;
    }

    public String getCsvReaderConfig() {
        // 为一个json格式
        return reader.csvReaderConfig;
    }

    @Override
    public String getTaskName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSourceEntityName() {
        throw new UnsupportedOperationException();
    }
}
