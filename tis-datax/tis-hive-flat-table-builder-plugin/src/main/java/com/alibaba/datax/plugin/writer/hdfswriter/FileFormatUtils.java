package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.plugin.task.util.DirtyRecord;
import com.google.common.collect.Lists;
import com.qlangtech.tis.hive.HdfsFileType;
import com.qlangtech.tis.hive.HdfsFormat;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-11 14:15
 **/
public class FileFormatUtils {


    /**
     * 写orcfile类型文件
     *
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public static void orcFileStartWrite(FileSystem fileSystem, JobConf conf, RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector) {
        List<IColMetaGetter> colsMeta = HdfsColMeta.getColsMeta(config);
        // List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, null);
        // List<String> columnNames = colsMeta.stream().map((c) -> c.getName()).collect(Collectors.toList());
        ColumnTypeValInspectors columnTypeInspectors = getColumnTypeInspectors(colsMeta);
        StructObjectInspector inspector = columnTypeInspectors.getStructObjectInspector();
//        ObjectInspectorFactory
//                .getStandardStructObjectInspector(columnNames, columnTypeInspectors.columnTypeInspectors);

        OrcSerde orcSerde = new OrcSerde();

        FileOutputFormat outFormat = new OrcOutputFormat();
        if (!"NONE".equalsIgnoreCase(compress) && null != compress) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, fileName, Reporter.NULL);
            Record record = null;
            Object[] rowVals = new Object[colsMeta.size()];
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Object[], Boolean> transportResult = transportOneRecord(record, rowVals, columnTypeInspectors.columnValGetters, taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(), orcSerde.serialize(transportResult.getLeft(), inspector));
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            HdfsHelper.LOG.error(message);
            Path path = new Path(fileName);
            HdfsHelper.deleteDir(fileSystem, path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    public static MutablePair<Object[], Boolean> transportOneRecord(Record record, Object[] rowVals, List<Function<Column, Object>> columnValGetters, TaskPluginCollector taskPluginCollector) {

        MutablePair<Object[], Boolean> transportResult = new MutablePair<Object[], Boolean>();
        transportResult.setRight(false);
        // List<Object> recordList = Lists.newArrayList();
        int recordLength = record.getColumnNumber();
        Function<Column, Object> colMeta = null;
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);

                //todo as method
                if (column.getRawData() != null) {
                    colMeta = columnValGetters.get(i);
                    try {
                        rowVals[i] = colMeta.apply(column);
                    } catch (Exception e) {
                        // warn: 此处认为脏数据
                        String message = String.format("字段类型转换错误：实际字段值为[%s].",
                                //colMeta.getType(),
                                column.toString());
                        taskPluginCollector.collectDirtyRecord(DirtyRecord.create(record), message);
                        transportResult.setRight(true);
                        break;
                    }
                } else {
                    // warn: it's all ok if nullFormat is null
                    // recordList.add(null);
                    rowVals[i] = null;// HdfsFormat.TEXT_FORMAT_NULL_FORMAT;
                }
            }
        }
        transportResult.setLeft(rowVals);
        return transportResult;
    }

//    public static List<Function<Column, Object>> getColumnValGetter(List<IColMetaGetter> colsMeta) {
//        List<Function<Column, Object>> valsGetter =
//                colsMeta.stream().map((getter) -> {
//                    getter.getType().accept()
//                });
//        return valsGetter;
//    }

    public static class ColumnTypeValInspectors {

        private final List<IColMetaGetter> colsMeta;

        private final List<ObjectInspector> columnTypeInspectors = Lists.newArrayList();
        public final List<Function<Column, Object>> columnValGetters = Lists.newArrayList();

        private void add(ObjectInspector objInspect, Function<Column, Object> colValGetter) {
            this.columnTypeInspectors.add(objInspect);
            this.columnValGetters.add(colValGetter);
        }

        public int getColsSize() {
            return colsMeta.size();
        }

        public List<Function<Column, Object>> getColumnValGetters() {
            return this.columnValGetters;
        }

        public StructObjectInspector getStructObjectInspector() {

            if (colsMeta.size() != columnTypeInspectors.size() || colsMeta.size() != columnValGetters.size()) {
                throw new IllegalStateException("colsMeta size:" + colsMeta.size()
                        + " is not equal with columnTypeInspectors.size():" + columnTypeInspectors.size()
                        + ", columnValGetters.size:" + columnValGetters.size());
            }

            List<String> columnNames = colsMeta.stream().map((c) -> c.getName()).collect(Collectors.toList());

            StructObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, this.columnTypeInspectors);
            return inspector;
        }

        public ColumnTypeValInspectors(List<IColMetaGetter> colsMeta) {
            this.colsMeta = colsMeta;
        }
    }

    /**
     * 根据writer配置的字段类型，构建inspector
     *
     * @param
     * @return
     */
    public static ColumnTypeValInspectors getColumnTypeInspectors(List<IColMetaGetter> colsMeta) {
        ColumnTypeValInspectors typeValInspectors = new ColumnTypeValInspectors(colsMeta);
        for (IColMetaGetter eachColumnConf : colsMeta) {

            eachColumnConf.getType().accept(new DataType.TypeVisitor<Void>() {
                @Override
                public Void bigInt(DataType type) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asLong());
                    return null;
                }

                @Override
                public Void doubleType(DataType type) {
                    typeValInspectors.add(
                            ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
                            , (col) -> col.asDouble());
                    return null;
                }

                @Override
                public Void dateType(DataType type) {

                    WritableDateObjectInspector inspector = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
                    //   typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> new java.sql.Date(col.asDate().getTime()));

                    typeValInspectors.add(inspector, (col) -> new org.apache.hadoop.hive.serde2.io.DateWritable(new java.sql.Date(col.asDate().getTime())));
                    return null;
                }

                @Override
                public Void timestampType(DataType type) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> new java.sql.Timestamp(col.asDate().getTime()));
                    return null;
                }

                @Override
                public Void timeType(DataType type) {
                    // throw new UnsupportedOperationException("col:" + eachColumnConf.getName() + " timeType is not support");
                    WritableTimestampObjectInspector inspector = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                    //  WritableHiveIntervalDayTimeObjectInspector inspector = PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector;
                    typeValInspectors.add(inspector, (col) -> {
                        Date time = col.asDate();
                        TimestampWritable val = new TimestampWritable(new java.sql.Timestamp(time.getTime()));
                        // int days, int hours, int minutes, int seconds, int nanos
                        //  val.set(0, time.getHours(), time.getMinutes(), time.getSeconds(), 0);
                        return val;
                    });
                    return null;
                }

                @Override
                public Void bitType(DataType type) {
                    return varcharType(type);
                }

                @Override
                public Void blobType(DataType type) {
                    JavaBinaryObjectInspector blobInspector = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
                    typeValInspectors.add(blobInspector, (col) -> {
                        return col.asBytes();
                    });
                    return null;
                    // throw new UnsupportedOperationException("blob is not support");
                }

                @Override
                public Void varcharType(DataType type) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asString());
                    return null;
                }

                @Override
                public Void intType(DataType type) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asLong().intValue());
                    return null;
                }

                @Override
                public Void floatType(DataType type) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asDouble().floatValue());
                    return null;
                }

                @Override
                public Void decimalType(DataType type) {
                    // org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
                    ObjectInspector inspector = PrimitiveObjectInspectorFactory
                            .getPrimitiveJavaObjectInspector(new DecimalTypeInfo(type.getColumnSize(), type.getDecimalDigits()));//.javaHiveDecimalObjectInspector;
                    typeValInspectors.add(inspector, (col) -> {
                        return HiveDecimal.create(col.asBigDecimal());
                    });
                    // typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(BigDecimal.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asDouble());
                    return null;
                }


                @Override
                public Void tinyIntType(DataType dataType) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asLong().byteValue());
                    return null;
                }

                @Override
                public Void smallIntType(DataType dataType) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asLong().shortValue());
                    return null;
                }

                @Override
                public Void boolType(DataType dataType) {
                    typeValInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), (col) -> col.asBoolean());
                    return null;
                }
            });
        }
        return typeValInspectors;
    }

    public static MutablePair<Text, Boolean> transportOneRecord(Record record, char fieldDelimiter, Object[] tmpRowVals, List<Function<Column, Object>> columnsConfiguration, TaskPluginCollector taskPluginCollector) {
        MutablePair<Object[], Boolean> transportResultList = transportOneRecord(record, tmpRowVals, columnsConfiguration, taskPluginCollector);
        //保存<转换后的数据,是否是脏数据>
        MutablePair<Text, Boolean> transportResult = new MutablePair<Text, Boolean>();
        transportResult.setRight(false);
        if (null != transportResultList) {
            Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), fieldDelimiter));
            transportResult.setRight(transportResultList.getRight());
            transportResult.setLeft(recordResult);
        }
        return transportResult;
    }

    public static Class<? extends CompressionCodec> getCompressCodec(String compress) {
        Class<? extends CompressionCodec> codecClass = null;
        if ("none".equalsIgnoreCase(compress) || null == compress) {
            codecClass = null;
        } else if ("GZIP".equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        } else if ("BZIP2".equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
        } else if ("SNAPPY".equalsIgnoreCase(compress)) {
            //todo 等需求明确后支持 需要用户安装SnappyCodec
            codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
            // org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class  not public
            //codecClass = org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class;
        } else {
            throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, String.format("目前不支持您配置的 compress 模式 : [%s]", compress));
        }
        return codecClass;
    }
}
