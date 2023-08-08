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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-14 17:03
 **/
public class CSVFormat extends BasicPainFormat {
    private static final Logger logger = LoggerFactory.getLogger(CSVFormat.class);
    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, validate = {}, advance = true)
    public String csvReaderConfig;

//    private Map<String, Object> parseCsvReaderConfig() {
//        return parseCsvReaderConfig(csvReaderConfig);
//    }
    private static Map<String, Object> parseCsvReaderConfig(String csvReaderConfig) {
        return JSON.parseObject(StringUtils.defaultString(csvReaderConfig, "{}"), new TypeReference<HashMap<String, Object>>() {
        });
    }

    @Override
    public UnstructuredWriter createWriter(Writer writer) {
        //Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter
        return new CsvWriterImpl(writer, this.getDateFormat(), this.nullFormat, this.getFieldDelimiter()) {
            @Override
            public void writeHeader(List<String> headers) throws IOException {
                if (header) {
                    this.writeOneRecord(headers);
                }
            }

            @Override
            public void writeOneRecord(List<String> splitedRows) throws IOException {
                this.csvWriter.writeRecord((String[]) splitedRows.toArray(new String[0]));
            }
        };
    }

    @Override
    public FileHeader readHeader(BufferedReader reader) throws IOException {

        TISCSVFormat csvFormat
                = (TISCSVFormat) this.createReader(reader);
        if (csvFormat.fileHeader != null) {
            return csvFormat.fileHeader;
        } else {
            if (csvFormat.hasNext()) {
                String[] row = csvFormat.next();
                return new FileHeader(row.length);
            } else {
                throw new IllegalStateException("must read row");
            }
        }
    }

    @Override
    public UnstructuredReader createReader(BufferedReader reader) {

        return new TISCSVFormat(reader);
    }

    private class TISCSVFormat extends com.alibaba.datax.plugin.unstructuredstorage.reader.CSVFormat {
        FileHeader fileHeader;

        public TISCSVFormat(BufferedReader reader) {
            super(reader);
        }

        @Override
        protected void setCsvReaderConfig(CsvReader csvReader) {

            // csvReader.setHeaders();
            // csvReader.
            csvReader.setDelimiter(getFieldDelimiter());
            Map<String, Object> csvReaderCfg = parseCsvReaderConfig(csvReaderConfig);
            if (MapUtils.isNotEmpty(csvReaderCfg)) {
                try {
                    BeanUtils.populate(csvReader, csvReaderCfg);
                    logger.info(String.format("csvReaderConfig设置成功,设置后CsvReader:%s", JSON.toJSONString(csvReader)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                //默认关闭安全模式, 放开10W字节的限制
                csvReader.setSafetySwitch(false);
                logger.info(String.format("CsvReader使用默认值[%s],csvReaderConfig值为[%s]"
                        , JSON.toJSONString(csvReader), JSON.toJSONString(csvReaderCfg)));
            }

            try {
                // CsvReader csvReader = csvFormat.reader;
                if (header) {
                    Objects.requireNonNull(csvReader, "csvFormat.reader can not be null");
                    if (csvReader.readHeaders()) {
                        fileHeader = new FileHeader(csvReader.getHeaderCount(), Lists.newArrayList(csvReader.getHeaders()));
                    } else {
                        throw new IllegalStateException("must contain head");
                    }
                }

//                else {
//                    if (this.hasNext()) {
//                        String[] row = this.next();
//                        fileHeader = new FileHeader(row.length);
//                    } else {
//                        throw new IllegalStateException("must read row");
//                    }
//                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean containHeader() {
        return false;
    }

    @TISExtension
    public static class Desc extends BasicPainFormatDescriptor {

        public boolean validateCsvReaderConfig(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            parseCsvReaderConfig(value);

            return PluginFieldValidators.validateCsvReaderConfig(msgHandler, context, fieldName, value);
        }

        @Override
        public String getDisplayName() {
            return "CSV";
        }
    }
}
