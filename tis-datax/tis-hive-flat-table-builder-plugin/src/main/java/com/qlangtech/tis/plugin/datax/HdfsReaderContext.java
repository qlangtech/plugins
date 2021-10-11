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

import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-13 13:31
 **/
public class HdfsReaderContext implements IDataxReaderContext {
    private final DataXHdfsReader reader;
    private final HdfsFileSystemFactory fileSystemFactory;

    public HdfsReaderContext(DataXHdfsReader reader) {
        this.reader = reader;
        this.fileSystemFactory = (HdfsFileSystemFactory) this.reader.getFs();
    }

    public String getDataXName() {
        return reader.dataXName;
    }

    @Override
    public String getTaskName() {
        return reader.dataXName;
    }

    @Override
    public String getSourceEntityName() {
        return reader.dataXName;
    }

    public String getDefaultFS() {
        return this.fileSystemFactory.hdfsAddress;
    }

    public String getPath() {
        Path path = new Path(new Path(this.fileSystemFactory.rootDir), new Path(this.reader.path));
        return path.toString();
    }

    public String getFileType() {
        return this.reader.fileType;
    }

    public String getColumn() {
        return this.reader.column;
    }

    public boolean isContainFieldDelimiter() {
        return StringUtils.isNotBlank(this.reader.fieldDelimiter);
    }

    public String getFieldDelimiter() {
        return this.reader.fieldDelimiter;
    }

    public boolean isContainEncoding() {
        return StringUtils.isNotBlank(this.reader.encoding);
    }

    public String getEncoding() {
        return this.reader.encoding;
    }

    public boolean isContainNullFormat() {
        return StringUtils.isNotBlank(this.reader.nullFormat);
    }

    public String getNullFormat() {
        return this.reader.nullFormat;
    }

    public boolean isContainCompress() {
        return StringUtils.isNotBlank(this.reader.compress);
    }

    public String getCompress() {
        return this.reader.compress;
    }

    public boolean isContainCsvReaderConfig() {
        return StringUtils.isNotBlank(this.reader.csvReaderConfig);
    }

    public String getCsvReaderConfig() {
        return this.reader.csvReaderConfig;
    }

    public String getTemplate() {
        return this.reader.template;
    }
}
