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
public class HadoopHFileInputFormat extends HadoopInputFormat<NullWritable, ArrayWritable> {
    public HadoopHFileInputFormat(String entityName, int colSize, HiveStoredAs serde, IHiveTableParams tableProperties) {
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
