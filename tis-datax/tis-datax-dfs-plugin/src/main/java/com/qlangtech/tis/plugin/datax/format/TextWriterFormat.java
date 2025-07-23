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

import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.writer.TextWriterImpl;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.CMeta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.List;

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-22 10:18
 **/
public class TextWriterFormat extends BasicPainFormat {
    @Override
    public UnstructuredWriter createWriter(Writer writer) {
        return new TextWriterImpl(writer, this.getDateFormat(), this.nullFormat, this.getFieldDelimiter()) {
            @Override
            public void writeHeader(List<String> headers) throws IOException {
                if (containHeader()) {
                    if (CollectionUtils.isEmpty(headers)) {
                        throw new IllegalArgumentException("header cols can not be empty");
                    }
                    this.writeOneRecord(headers);
                }
            }
            @Override
            public void writeOneRecord(List<String> splitedRows) throws IOException {
                this.textWriter.write(String.format("%s%s", StringUtils.join(splitedRows, getFieldDelimiter()), IOUtils.LINE_SEPARATOR));
            }
        };
    }
    @Override
    public UnstructuredReader createReader(InputStream input, List<CMeta> cols) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileHeader readHeader(InputStream input) throws IOException {
        throw new UnsupportedOperationException();
    }

    @TISExtension
    public static class Desc extends BasicPainFormatDescriptor {
        @Override
        public String getDisplayName() {
            return "TEXT";
        }
    }
}
