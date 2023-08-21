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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.plugin.datax.format.CSVFormat;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-01 14:52
 **/
public class DataXDFSReaderContext implements IDataxReaderContext {

    public static final String FTP_TASK = "ftp_datax";
    protected final IDFSReader reader;
    //  private final FTPServer ftpServer;

    @Override
    public String getReaderContextId() {
        return "ftp";
    }

    public DataXDFSReaderContext(IDFSReader reader) {
        this.reader = reader;
        Objects.requireNonNull(this.reader.getDfsLinker(), "reader.linker can not be null");
        // this.ftpServer = FTPServer.getServer(this.reader.linker);
    }

//    public String getProtocol() {
//        return this.ftpServer.protocol;
//    }
//
//    public String getHost() {
//        return this.ftpServer.host;
//    }
//
//    public boolean isContainPort() {
//        return this.ftpServer.port != null;
//    }
//
//    public Integer getPort() {
//        return this.ftpServer.port;
//    }
//
//    public boolean isContainTimeout() {
//        return this.ftpServer.timeout != null;
//    }
//
//    public Integer getTimeout() {
//        return this.ftpServer.timeout;
//    }
//
//    public boolean isContainConnectPattern() {
//        return StringUtils.isNotBlank(this.ftpServer.connectPattern);
//    }
//
//    public String getConnectPattern() {
//        return this.ftpServer.connectPattern;
//    }
//
//    public String getFormat() {
//        return this.reader.fileFormat.getFormat();
//    }
//
//    public String getUsername() {
//        return this.ftpServer.username;
//    }
//
//    public String getPassword() {
//        return this.ftpServer.password;
//    }

    public String getPath() {
        return this.reader.getDfsLinker().getRootPath();
    }

//    public String getColumn() {
//        return this.reader.column;
//    }

//    public String getFieldDelimiter() {
//        return this.reader.fileFormat.getFieldDelimiter();
//    }
//
//    public boolean isContainFieldDelimiter() {
//        return StringUtils.isNotBlank(this.reader.fileFormat.getFieldDelimiter());
//    }

    public boolean isContainCompress() {
        // return StringUtils.isNotBlank(this.reader.compress);
        return false;
    }

    public String getCompress() {
        throw new UnsupportedOperationException();
    }

    public boolean isContainEncoding() {
        return false;
    }

    public String getEncoding() {
        throw new UnsupportedOperationException();
    }

    public boolean isContainSkipHeader() {
        return true;
    }

//    public Boolean getSkipHeader() {
//       // return !this.reader.getFileFormat().containHeader();
//    }

    public boolean isContainNullFormat() {
        //  return StringUtils.isNotBlank(this.reader.nullFormat);
        return false;
    }

    public boolean isContainCsvReaderConfig() {
        return true;
//        if (!(this.reader.getFileFormat() instanceof CSVFormat)) {
//            return false;
//        }
//        return StringUtils.isNotBlank(((CSVFormat) this.reader.getFileFormat()).csvReaderConfig);
    }

//    public String getCsvReaderConfig() {
//        return ((CSVFormat) this.reader.getFileFormat()).csvReaderConfig;
//    }

    @Override
    public String getTaskName() {
        return FTP_TASK;
    }

    @Override
    public String getSourceEntityName() {
        return null;
    }

    @Override
    public String getSourceTableName() {
        return null;
    }
}
