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

import com.alibaba.datax.plugin.unstructuredstorage.reader.TEXTFormat;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.writer.TextWriterImpl;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-14 17:03
 **/
public class TextFormat extends BasicPainFormat {


    @Override
    public UnstructuredWriter createWriter(Writer writer) {

        //Writer writer, DateFormat dateParse, String nullFormat, char fieldDelimiter
        return new TextWriterImpl(writer, this.getDateFormat(), this.nullFormat, this.getFieldDelimiter()) {
            @Override
            public void writeHeader(List<String> headers) throws IOException {
                if (header) {
                    this.writeOneRecord(headers);
                }
            }

            @Override
            public void writeOneRecord(List<String> splitedRows) throws IOException {
                this.textWriter.write(String.format("%s%s",
                        StringUtils.join(splitedRows, getFieldDelimiter()),
                        IOUtils.LINE_SEPARATOR));
            }
        };
    }

    @Override
    public UnstructuredReader createReader(BufferedReader reader) {
        return new TEXTFormat(reader, !header, getFieldDelimiter());
    }

    @Override
    public FileHeader readHeader(BufferedReader reader) throws IOException {
        TEXTFormat textFormat = (TEXTFormat) this.createReader(reader);
        String[] header = textFormat.getHeader();
        int colCount;
        if (header == null) {
            if (textFormat.hasNext()) {
                colCount = textFormat.next().length;
            } else {
                throw new IllegalStateException("can not read content from textFormat");
            }
        } else {
            colCount = header.length;
        }
        return new FileHeader(colCount, header == null ? null : Lists.newArrayList(header));
    }


    @Override
    public boolean containHeader() {
        return this.header;
    }

    @TISExtension
    public static class Desc extends BasicPainFormatDescriptor {
        @Override
        public String getDisplayName() {
            return "TEXT";
        }
    }
}
