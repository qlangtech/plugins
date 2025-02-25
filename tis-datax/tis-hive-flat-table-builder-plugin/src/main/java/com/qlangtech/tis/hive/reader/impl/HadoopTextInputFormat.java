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

package com.qlangtech.tis.hive.reader.impl;

import com.qlangtech.tis.config.hive.meta.IHiveTableParams;
import com.qlangtech.tis.hive.DefaultHiveMetaStore.HiveStoredAs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-08 14:06
 **/
public class HadoopTextInputFormat extends HadoopInputFormat<LongWritable, Text> {

    private static final String KEY_COMRESSION_CODEC = "compression.codec";

    public HadoopTextInputFormat(String entityName, int colSize
                                 //  , FileInputFormat inputFormat, FileOutputFormat outputFormat,
            , HiveStoredAs serde, IHiveTableParams tableParams) {
        super(entityName, colSize, serde, tableParams, serde.getJobConf());
        if (this.inputFormat instanceof org.apache.hadoop.mapred.TextInputFormat) {
            ((org.apache.hadoop.mapred.TextInputFormat) this.inputFormat).configure(conf);
        }
    }

    /**
     * <pre>
     * CREATE TABLE employee (
     *     id INT,
     *     name STRING,
     *     position STRING,
     *     hire_date STRING
     * )
     * ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     * WITH SERDEPROPERTIES (
     *   'field.delim'='\001',
     *   'serialization.format'='\001'
     * )
     * STORED AS TEXTFILE
     * LOCATION '/user/hive/warehouse/mydb/employee_gzip'
     * TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
     * </pre>
     *
     * @return
     */
    @Override
    protected boolean isCompressed() {

//        try {
//            String codecClass = this.tableProperties.getTabParameter(KEY_COMRESSION_CODEC);
//            if (StringUtils.isNotEmpty(codecClass)) {
//                org.apache.hadoop.mapred.FileOutputFormat.setOutputCompressorClass(this.conf
//                        , (Class<? extends CompressionCodec>) HadoopTextInputFormat.class.getClassLoader().loadClass(codecClass));
//                return true;
//            }
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }

        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue(int colSize) {
        return new Text();
    }
}
