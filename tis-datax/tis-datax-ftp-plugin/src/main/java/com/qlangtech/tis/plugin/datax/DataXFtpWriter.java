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

import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.ftpwriter.FtpWriter
 **/
@Public
public class DataXFtpWriter extends DataxWriter {
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String protocol;
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String host;
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer port;
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer timeout;
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String username;
    @FormField(ordinal = 5, type = FormFieldType.PASSWORD, validate = {Validator.require})
    public String password;
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String path;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
//    public String fileName;
    @FormField(ordinal = 8, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;
    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {})
    public String fieldDelimiter;
    //    @FormField(ordinal = 10, type = FormFieldType.INPUTTEXT, validate = {})
//    public String compress;
    @FormField(ordinal = 11, type = FormFieldType.ENUM, validate = {})
    public String encoding;
    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {})
    public String nullFormat;
    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, validate = {})
    public String dateFormat;
    @FormField(ordinal = 14, type = FormFieldType.ENUM, validate = {})
    public String fileFormat;
    @FormField(ordinal = 15, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String suffix;
    @FormField(ordinal = 16, type = FormFieldType.ENUM, validate = {})
    public Boolean header;

    @FormField(ordinal = 17, type = FormFieldType.TEXTAREA,advance = false , validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXFtpWriter.class, "DataXFtpWriter-tpl.json");
    }


    @Override
    public String getTemplate() {
        return this.template;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        DataXFtpWriterContext writerContext = new DataXFtpWriterContext(this, tableMap.get());
        return writerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.FTP;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DataXFtpReader.DATAX_NAME;
        }
    }
}
