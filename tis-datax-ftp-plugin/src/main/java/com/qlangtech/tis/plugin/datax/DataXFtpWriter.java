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
public class DataXFtpWriter extends DataxWriter {
    //private static final String DATAX_NAME = "FTP";

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

    @FormField(ordinal = 17, type = FormFieldType.TEXTAREA, validate = {Validator.require})
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
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DataXFtpReader.DATAX_NAME;
        }
    }
}
