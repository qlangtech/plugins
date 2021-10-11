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
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-01 14:52
 **/
public class DataXFtpReaderContext implements IDataxReaderContext {

    private final DataXFtpReader reader;

    public DataXFtpReaderContext(DataXFtpReader reader) {
        this.reader = reader;
    }

    public String getProtocol() {
        return this.reader.protocol;
    }
    public String getHost() {
        return this.reader.host;
    }
    public boolean isContainPort(){
        return this.reader.port!= null;
    }
    public Integer getPort() {
        return this.reader.port;
    }
    public boolean isContainTimeout(){
        return this.reader.timeout!= null;
    }
    public Integer getTimeout() {
        return this.reader.timeout;
    }
    public boolean isContainConnectPattern(){
        return StringUtils.isNotBlank(this.reader.connectPattern);
    }
    public String getConnectPattern() {
        return this.reader.connectPattern;
    }
    public String getUsername() {
        return this.reader.username;
    }
    public String getPassword() {
        return this.reader.password;
    }
    public String getPath() {
        return this.reader.path;
    }
    public String getColumn() {
        return this.reader.column;
    }
    public String getFieldDelimiter() {
        return this.reader.fieldDelimiter;
    }
    public boolean isContainCompress(){
        return StringUtils.isNotBlank(this.reader.compress);
    }
    public String getCompress() {
        return this.reader.compress;
    }
    public boolean isContainEncoding(){
        return StringUtils.isNotBlank(this.reader.encoding);
    }
    public String getEncoding() {
        return this.reader.encoding;
    }
    public boolean isContainSkipHeader(){
        return this.reader.skipHeader!= null;
    }
    public Boolean getSkipHeader() {
        return this.reader.skipHeader;
    }
    public boolean isContainNullFormat(){
        return StringUtils.isNotBlank(this.reader.nullFormat);
    }
    public String getNullFormat() {
        return this.reader.nullFormat;
    }
    public boolean isContainMaxTraversalLevel(){
        return StringUtils.isNotBlank(this.reader.maxTraversalLevel);
    }
    public String getMaxTraversalLevel() {
        return this.reader.maxTraversalLevel;
    }
    public boolean isContainCsvReaderConfig(){
        return StringUtils.isNotBlank(this.reader.csvReaderConfig);
    }
    public String getCsvReaderConfig() {
        return this.reader.csvReaderConfig;
    }



    @Override
    public String getTaskName() {
        return null;
    }

    @Override
    public String getSourceEntityName() {
        return null;
    }
}
