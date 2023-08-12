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
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
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
        return new TISCsvWriterImpl(writer, this.getDateFormat(), this.nullFormat, this.getFieldDelimiter());
    }

    private class TISCsvWriterImpl extends CsvWriterImpl {
        public TISCsvWriterImpl(Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter) {
            super(writer, dateParse, nullFormat, fieldDelimiter);
            Map<String, Object> csvCfg = parseCsvReaderConfig(csvReaderConfig);

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

//    @Override
//    protected FileHeader readHeader(UnstructuredReader reader) throws IOException {
//        UnstructuredReader csvFormat = reader;
//        String[] fileHeader = null;
//        int colCount;
//        if (csvFormat.getHeader() != null) {
//            fileHeader = csvFormat.getHeader();
//        } else {
//            if (csvFormat.hasNext()) {
//                String[] row = csvFormat.next();
//                colCount = row.length;
//                //return new FileHeader(row.length);
//            } else {
//                throw new IllegalStateException("must read row");
//            }
//        }
//    }

    @Override
    public UnstructuredReader createReader(BufferedReader reader) {

        return new TISCSVFormat(reader);
    }

    private class TISCSVFormat extends com.alibaba.datax.plugin.unstructuredstorage.reader.CSVFormat {
        String[] fileHeader;

        public TISCSVFormat(BufferedReader reader) {
            super(reader);
        }

        @Override
        public String[] getHeader() {
            return this.fileHeader;
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
                        this.fileHeader = csvReader.getHeaders();// new FileHeader(csvReader.getHeaderCount(), Lists.newArrayList());
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
