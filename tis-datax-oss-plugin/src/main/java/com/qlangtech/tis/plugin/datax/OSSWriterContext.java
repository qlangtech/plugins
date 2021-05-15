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
 * @create: 2021-05-14 11:59
 **/
public class OSSWriterContext implements IDataxReaderContext {

    private final DataXOssWriter writer;

    public OSSWriterContext(DataXOssWriter ossWriter) {
        this.writer = ossWriter;
    }

    public IAliyunToken getOss() {
        return writer.getOSSConfig();
    }

    public String getBucket() {
        return writer.bucket;
    }

    public String getObject() {
        return writer.object;
    }

    public String getWriteMode() {
        return writer.writeMode;
    }

    public String getFieldDelimiter() {
        return writer.fieldDelimiter;
    }

    public String getEncoding() {
        return writer.encoding;
    }

    public String getNullFormat() {
        return writer.nullFormat;
    }

    public String getDateFormat() {
        return this.writer.dateFormat;
    }

    public String getFileFormat() {
        return this.writer.fileFormat;
    }

    public String getHeader() {
        return this.writer.header;
    }

    public Integer getMaxFileSize() {
        return this.writer.maxFileSize;
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
