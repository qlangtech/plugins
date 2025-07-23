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
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.writer.CsvWriterImpl;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.List;
import java.util.Map;

import static com.qlangtech.tis.plugin.datax.format.CSVReaderFormat.parseCsvReaderConfig;

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-22 10:17
 **/
public class CSVWriterFormat extends BasicPainFormat {
    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, validate = {}, advance = true)
    public String csvWriterConfig;

    @Override
    public UnstructuredWriter createWriter(Writer writer) {
        //Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter
        return new TISCsvWriterImpl(writer, this.getDateFormat(), this.nullFormat, this.getFieldDelimiter());
    }

    private class TISCsvWriterImpl extends CsvWriterImpl {
        public TISCsvWriterImpl(Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter) {
            super(writer, dateParse, nullFormat, fieldDelimiter);
            Map<String, Object> csvCfg = parseCsvReaderConfig(csvWriterConfig);

            if (MapUtils.isNotEmpty(csvCfg)) {
                try {

                    BeanUtils.populate(csvWriter, csvCfg);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void writeHeader(List<String> headers) throws IOException {
            if (containHeader()) {
                if (CollectionUtils.isEmpty(headers)) {
                    throw new IllegalArgumentException("header cols can not be null");
                }
                this.writeOneRecord(headers);
            }
        }

        @Override
        public void writeOneRecord(List<String> splitedRows) throws IOException {
            this.csvWriter.writeRecord((String[]) splitedRows.toArray(new String[0]));
        }
    }

    @Override
    public UnstructuredReader createReader(InputStream input, List<CMeta> cols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileHeader readHeader(InputStream input) throws IOException {
        throw new UnsupportedEncodingException();
    }

    @TISExtension
    public static class Desc extends BasicPainFormatDescriptor {

        public boolean validateCsvWriterConfig(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            parseCsvReaderConfig(value);

            return PluginFieldValidators.validateCsvReaderConfig(msgHandler, context, fieldName, value);
        }

        @Override
        public String getDisplayName() {
            return "CSV";
        }
    }
}
