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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-08 14:15
 **/
public class HadoopOrcInputFormat extends HadoopInputFormat<NullWritable, ArrayWritable> {
    public HadoopOrcInputFormat(String entityName, int colSize, HiveStoredAs serde, IHiveTableParams tableProperties) {
        super(entityName, colSize, serde, tableProperties, serde.getJobConf());
    }

    @Override
    protected boolean isCompressed() {
        return false;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public ArrayWritable createValue(int colSize) {
        ArrayWritable value = new ArrayWritable(Text.class, new Writable[colSize]);
        return value;
    }
}
