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

package com.qlangtech.tis.plugin.datax.hudi;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.datax.plugin.writer.hudi.TisDataXHudiWriter;
import com.qlangtech.tis.extension.impl.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-17 09:43
 **/
public class TestDataXHudiWriterTask {

    @Test
    public void testCsvWrite() throws Exception {
        DefaultRecord[] record = new DefaultRecord[1];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        TisDataXHudiWriter.Task task = new TisDataXHudiWriter.Task() {
            @Override
            public void init() {
                this.fileType = "csv";
                this.writerSliceConfig
                        = Configuration.from(
                        IOUtils.loadResourceFromClasspath(
                                TestDataXHudiWriterTask.class, TestDataXHudiWriter.hudi_datax_writer_assert_without_optional))
                        .getConfiguration("parameter");

                List<HdfsColMeta> colsMeta = HdfsColMeta.getColsMeta(this.writerSliceConfig);

                record[0] = new DefaultRecord();
                for (HdfsColMeta col : colsMeta) {
                    // col.csvType
                    switch (col.csvType) {
                        case STRING:
                            record[0].addColumn(new StringColumn("{\"name\":\"" + RandomStringUtils.randomAlphanumeric(4) + "\"}"));
                            break;
                        case BOOLEAN:
                            break;
                        case NUMBER:
                            record[0].addColumn(new LongColumn((long) (Math.random() * 1000)));
                            break;
                    }

                }
            }

            @Override
            public void prepare() {
                super.prepare();
            }

            @Override
            protected OutputStream getOutputStream(Path targetPath) {
                return output;
            }
        };

        RecordReceiver records = new RecordReceiver() {
            int index = 0;

            @Override
            public Record getFromReader() {
                if (index++ < 1) {
                    return record[0];
                }
                return null;
            }

            @Override
            public void shutdown() {

            }
        };
        task.init();
        task.prepare();
        task.startWrite(records);

        System.out.println(new String(output.toByteArray()));
    }
}
