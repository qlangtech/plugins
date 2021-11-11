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

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-01 16:52
 **/
public class DataXFtpWriterContext implements IDataxContext {
    private final DataXFtpWriter writer;
    private final IDataxProcessor.TableMap tableMapper;

    public DataXFtpWriterContext(DataXFtpWriter writer, IDataxProcessor.TableMap tableMapper) {
        this.writer = writer;
        this.tableMapper = tableMapper;
    }

    public String getProtocol() {
        return this.writer.protocol;
    }

    public String getHost() {
        return this.writer.host;
    }

    public boolean isContainPort() {
        return this.writer.port != null;
    }

    public Integer getPort() {
        return this.writer.port;
    }

    public boolean isContainTimeout() {
        return this.writer.timeout != null;
    }

    public Integer getTimeout() {
        return this.writer.timeout;
    }

    public String getUsername() {
        return this.writer.username;
    }

    public String getPassword() {
        return this.writer.password;
    }

    public String getPath() {
        return this.writer.path;
    }

    public String getFileName() {
        return this.tableMapper.getTo();
    }

    public String getWriteMode() {
        return this.writer.writeMode;
    }

    public boolean isContainFieldDelimiter() {
        return StringUtils.isNotBlank(this.writer.fieldDelimiter);
    }

    public String getFieldDelimiter() {
        return this.writer.fieldDelimiter;
    }

    public boolean isContainEncoding() {
        return StringUtils.isNotBlank(this.writer.encoding);
    }

    public String getEncoding() {
        return this.writer.encoding;
    }

    public boolean isContainNullFormat() {
        return StringUtils.isNotBlank(this.writer.nullFormat);
    }

    public String getNullFormat() {
        return this.writer.nullFormat;
    }

    public boolean isContainDateFormat() {
        return StringUtils.isNotBlank(this.writer.dateFormat);
    }

    public String getDateFormat() {
        return this.writer.dateFormat;
    }

    public boolean isContainFileFormat() {
        return StringUtils.isNotBlank(this.writer.fileFormat);
    }

    public String getFileFormat() {
        return this.writer.fileFormat;
    }

    public boolean isContainSuffix() {
        return StringUtils.isNotBlank(this.writer.suffix);
    }

    public String getSuffix() {
        return this.writer.suffix;
    }

    public boolean isContainHeader() {
        List<ISelectedTab.ColMeta> cols = tableMapper.getSourceCols();
        return (this.writer.header != null && this.writer.header && cols.size() > 0);
    }

    public String getHeader() {
        return tableMapper.getSourceCols().stream().map((c) -> "'" + c.getName() + "'").collect(Collectors.joining(","));
    }

}
