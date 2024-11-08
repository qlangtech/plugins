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

import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.qlangtech.tis.plugin.datax.format.TextFormat;
import com.qlangtech.tis.plugin.ds.CMeta;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-07 15:55
 * @see com.qlangtech.tis.hive.reader.HiveDFSLinker#getInputFileFormat Create By
 **/
public class HadoopInputFormat extends TextFormat {
    private final org.apache.hadoop.mapred.TextInputFormat inputFormat;
    private final JobConf conf;
    private final String entityName;
    private final SerDe serde;

    public HadoopInputFormat(String entityName, TextInputFormat inputFormat, SerDe serde, JobConf conf) {
        this.entityName = entityName;
        this.inputFormat = Objects.requireNonNull(inputFormat, "inputFormat can not be null");
        this.conf = Objects.requireNonNull(conf, "conf can not be null");
        this.serde = Objects.requireNonNull(serde, "serde can not be null");
    }

    public final String getEntityName() {
        return this.entityName;
    }

    private InputSplit[] getSplits() throws IOException {
        return inputFormat.getSplits(conf, 1);
    }

    public SerDe getSerde() {
        return this.serde;
    }

    public RecordReader<LongWritable, Text> getRecordReader(Path inputPath) throws IOException {
        FileInputFormat.addInputPath(conf, inputPath);
        InputSplit[] splits = this.getSplits();
        if (splits.length < 1) {
            throw new IllegalStateException("splits.length can not small 1");
        }
        return inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);
    }

    @Override
    public UnstructuredWriter createWriter(Writer writer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnstructuredReader createReader(BufferedReader reader, List<CMeta> sourceCols) {
        throw new UnsupportedOperationException();
    }
}
