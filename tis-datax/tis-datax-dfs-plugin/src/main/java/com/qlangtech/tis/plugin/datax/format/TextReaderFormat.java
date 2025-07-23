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
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.CMeta;

import java.io.BufferedReader;
import java.io.Writer;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-14 17:03
 **/
public class TextReaderFormat extends BasicReaderPainFormat {


    @Override
    public UnstructuredReader createReader(BufferedReader reader, List<CMeta> sourceCols) {
        return new TEXTFormat(reader, !header, sourceCols.size(), getFieldDelimiter());
    }

    @Override
    protected UnstructuredWriter createWriter(Writer writer) {
        throw new UnsupportedOperationException();
    }


    @TISExtension
    public static class Desc extends BasicPainFormatDescriptor {
        @Override
        public boolean isSupportWriterConnector() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return "TEXT";
        }
    }
}
