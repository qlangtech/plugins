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

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-10 09:46
 **/
public class BasicFormatTest {
    SimpleDateFormat dataFormatYYYYMMdd;

    protected String testWithInsert(BasicPainFormat format) throws Exception {

        dataFormatYYYYMMdd = format.getDateFormat();
        //  String today = "2023-08-09";
        // Assert.assertEquals(today, format.getDateFormat().format(dataFormatYYYYMMdd.parse(today)));

        StringWriter writer = new StringWriter();
        UnstructuredWriter txtWrite = format.createWriter(writer);


        List<Record> records = createExampleRecords();

        List<String> header = Lists.newArrayList("id", "name", "age", "is_active", "birthdate");

        txtWrite.writeHeader(header);

        for (Record r : records) {
            txtWrite.writeOneRecord(r);
        }
        String writeContent = (writer.toString());
        System.out.println(writeContent);
        return writeContent;
    }

    private List<Record> createExampleRecords() {
        return IOUtils.loadResourceFromClasspath(
                TestTextFormat.class, "tbl.text", true, (read) -> {

                    BufferedReader reader = org.apache.commons.io.IOUtils.toBufferedReader(new InputStreamReader(read, TisUTF8.get()));
                    List<Record> rows = Lists.newArrayList();
                    DefaultRecord record = null;
                    String line = null;
                    String[] row = null;
                    DataType[] types = new DataType[]{DataType.getType(JDBCTypes.INTEGER)
                            , DataType.createVarChar(32)
                            , DataType.getType(JDBCTypes.INTEGER)
                            , DataType.getType(JDBCTypes.FLOAT)
                            , DataType.getType(JDBCTypes.INTEGER)
                            , DataType.getType(JDBCTypes.DATE)};

                    while ((line = reader.readLine()) != null) {
                        row = StringUtils.split(line, Delimiter.Tab.val);
                        record = new DefaultRecord();
                        for (int i = 0; i < row.length; i++) {
                            record.addColumn(parseColum(types[i], row[i]));
                        }
                        rows.add(record);
                    }
                    return rows;
                });
    }

    private Column parseColum(DataType type, String literia) {
        return type.accept(new DataType.TypeVisitor<Column>() {
            @Override
            public Column bigInt(DataType type) {
                return new LongColumn(literia);
            }

            @Override
            public Column doubleType(DataType type) {
                return new DoubleColumn(literia);
            }

            @Override
            public Column dateType(DataType type) {
                try {
                    return new DateColumn(dataFormatYYYYMMdd.parse(literia));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Column intType(DataType type) {
                return new LongColumn(literia);
            }

            @Override
            public Column floatType(DataType type) {
                return doubleType(type);
            }

            @Override
            public Column decimalType(DataType type) {
                return doubleType(type);
            }

            @Override
            public Column timeType(DataType type) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column tinyIntType(DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column smallIntType(DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column boolType(DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column timestampType(DataType type) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column bitType(DataType type) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column blobType(DataType type) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Column varcharType(DataType type) {
                return new StringColumn(literia);
            }
        });
    }
}
