package com.qlangtech.tis.hive.reader.impl;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-08 14:15
 **/
public class HadoopHFileInputFormat extends HadoopInputFormat<NullWritable, ArrayWritable> {
    public HadoopHFileInputFormat(String entityName, int colSize
            , org.apache.hadoop.mapred.InputFormat inputFormat, AbstractSerDe serde, JobConf conf) {
        super(entityName, colSize, inputFormat, serde, conf);
    }

    @Override
    protected NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    protected ArrayWritable createValue(int colSize) {
        ArrayWritable value = new ArrayWritable(Text.class, new Writable[colSize]);
        return value;
    }
}
