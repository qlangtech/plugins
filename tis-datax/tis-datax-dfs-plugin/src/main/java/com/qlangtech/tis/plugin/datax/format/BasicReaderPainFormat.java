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

import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.guesstype.GuessFieldType;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-22 10:08
 **/
public abstract class BasicReaderPainFormat extends BasicPainFormat {

    @FormField(ordinal = 17, validate = {Validator.require})
    public GuessFieldType guessFieldType;

    protected abstract UnstructuredReader createReader(BufferedReader reader, List<CMeta> sourceCols) throws IOException;

    @Override
    public final FileHeader readHeader(InputStream input) throws IOException {
        return readHeader(createReader(input, Collections.emptyList()));
    }

    // protected abstract FileHeader readHeader(UnstructuredReader reader) throws IOException;
    @Override
    public UnstructuredReader createReader(InputStream input, List<CMeta> sourceCols) {
        try {
            return createReader(Compress.parse(compress).decorate(input, encoding), sourceCols);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // @Override
    protected final FileHeader readHeader(UnstructuredReader reader) throws IOException {
        UnstructuredReader textFormat = reader;
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

        // guess all col types
        DataType[] types = new DataType[colCount];
        Objects.requireNonNull(this.guessFieldType, "guessFieldType can not be null")
                .processUnStructGuess(types, this, textFormat);
        return new FileHeader(colCount, header == null ? null : Lists.newArrayList(header), Lists.newArrayList(types));
    }
}
