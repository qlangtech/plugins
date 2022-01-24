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

package com.alibaba.datax.plugin.writer.hudi;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.*;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.qlangtech.tis.plugin.datax.TisDataXHdfsWriter;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-22 15:16
 **/
public class TisDataXHudiWriter extends HdfsWriter {
    public static class Job extends TisDataXHdfsWriter.Job {

    }

    public static class Task extends TisDataXHdfsWriter.Task {

        ObjectWriter csvObjWriter = null;
        CsvSchema.Builder csvSchemaBuilder = null;

        @Override
        public void init() {
            super.init();

        }

        @Override
        public void prepare() {
            super.prepare();
            this.csvSchemaBuilder = CsvSchema.builder();

            List<ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType>>
                    colsMeta = HdfsHelper.getColsMeta(this.writerSliceConfig);
            for (ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType> col : colsMeta) {
                csvSchemaBuilder.addColumn(col.left, parseCsvType(col));
            }
            csvObjWriter = new CsvMapper()
                    .setSerializerFactory(new TISSerializerFactory(colsMeta))
                    .writerFor(Record.class)
                    .with(csvSchemaBuilder.setUseHeader(true).setColumnSeparator(',').build());
        }


        private CsvSchema.ColumnType parseCsvType(ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType> col) {
            switch (col.right) {
                case STRING:
                    return CsvSchema.ColumnType.STRING;
                case BOOLEAN:
                    return CsvSchema.ColumnType.BOOLEAN;
                case NUMBER:
                    return CsvSchema.ColumnType.NUMBER;
            }
            throw new IllegalStateException("illegal csv type:" + col.right);
        }


        @Override
        protected void csvFileStartWrite(
                RecordReceiver lineReceiver, Configuration config
                , String fileName, TaskPluginCollector taskPluginCollector) {
            try {
                Path targetPath = new Path(hdfsHelper.conf.getWorkingDirectory()
                        , this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE)
                        + "/" + this.fileName);
                FSDataOutputStream output = this.hdfsHelper.getOutputStream(targetPath);
                SequenceWriter sequenceWriter = csvObjWriter.writeValues(output);
                Record record = null;
                while ((record = lineReceiver.getFromReader()) != null) {
                    sequenceWriter.write(record);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }
}

