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

package com.qlangtech.tis.plugin.datax.format;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:49
 **/
public abstract class BasicPainFormat extends FileFormat {
    private static final Logger logger = LoggerFactory.getLogger(BasicPainFormat.class);
    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String dateFormat;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String nullFormat;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String fieldDelimiter;

    @FormField(ordinal = 16, type = FormFieldType.ENUM, validate = {})
    public boolean header;


    protected final SimpleDateFormat getDateFormat() {
        return parseFormat(this.dateFormat);
    }

    public static SimpleDateFormat parseFormat(String dateFormat) {
        return new SimpleDateFormat(dateFormat);
    }

    @Override
    public final char getFieldDelimiter() {
        return (Delimiter.parse(this.fieldDelimiter).val);
    }

    @Override
    public Descriptor<FileFormat> getDescriptor() {
        Descriptor<FileFormat> descriptor = super.getDescriptor();
        if (!BasicPainFormatDescriptor.class.isAssignableFrom(descriptor.getClass())) {
            throw new IllegalStateException(descriptor.getClass().getName() + " must extend form " + BasicPainFormatDescriptor.class.getName());
        }
        return descriptor;
    }

    protected static class BasicPainFormatDescriptor extends Descriptor<FileFormat> {


        public boolean validateDateFormat(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                SimpleDateFormat dateFormat = BasicPainFormat.parseFormat(value);
                dateFormat.format(new Date());
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, "format格式不正确");
                return false;
            }
            return true;
        }

    }
}
