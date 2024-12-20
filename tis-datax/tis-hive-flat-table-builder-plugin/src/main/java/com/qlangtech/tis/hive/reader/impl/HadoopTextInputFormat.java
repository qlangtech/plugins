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

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-08 14:06
 **/
public class HadoopTextInputFormat extends HadoopInputFormat<LongWritable, Text> {
    public HadoopTextInputFormat(String entityName, int colSize, FileInputFormat inputFormat, AbstractSerDe serde, JobConf conf) {
        super(entityName, colSize, inputFormat, serde, conf);
    }

    @Override
    protected LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    protected Text createValue(int colSize) {
        return new Text();
    }
}
