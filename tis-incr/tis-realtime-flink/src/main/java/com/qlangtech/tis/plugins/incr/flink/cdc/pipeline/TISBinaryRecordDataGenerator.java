package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.transformer.SingleTableEventTransformerMapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.serializer.InternalSerializers;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryRecordDataWriter;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryWriter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-15 23:58
 * @see org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator copy from for small modify for exection error info
 **/
public class TISBinaryRecordDataGenerator {
    private final DataType[] dataTypes;
    private final TypeSerializer[] serializers;
    private final List<FlinkCol> cols;
    //  public final FlinkCol2Index flinkCol2Index;
    // private final Optional<List<UDFDefinition>> tisUDFDefinition;
    private transient BinaryRecordData reuseRecordData;
    private transient BinaryRecordDataWriter reuseWriter;

    private final Optional<SingleTableEventTransformerMapper> tableEventTransformerMapper;


    public TISBinaryRecordDataGenerator(DataType[] dataTypes, List<FlinkCol> cols, Optional<List<UDFDefinition>> tisUDFDefinition) {
        this(
                dataTypes,
                Arrays.stream(dataTypes)
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new), cols, tisUDFDefinition);
    }

    public TISBinaryRecordDataGenerator(DataType[] dataTypes, TypeSerializer[] serializers, List<FlinkCol> cols, Optional<List<UDFDefinition>> tisUDFDefinition) {
        checkArgument(
                dataTypes.length == serializers.length,
                "The types and serializers must have the same length. But types is %s and serializers is %s",
                dataTypes.length,
                serializers.length);

        checkArgument(
                dataTypes.length == cols.size(),
                "The types and cols must have the same length. But types is %s and cols is %s",
                dataTypes.length,
                cols.size());

        this.dataTypes = dataTypes;
        this.serializers = serializers;
        this.cols = cols;
        // this.tisUDFDefinition = Objects.requireNonNull(tisUDFDefinition, "tisUDFDefinition can not be null");

        this.reuseRecordData = new BinaryRecordData(dataTypes.length);
        this.reuseWriter = new BinaryRecordDataWriter(reuseRecordData);
        // this.flinkCol2Index = Objects.requireNonNull(flinkCol2Index, "flinkCol2Index can not be null");

        //FlinkCol2Index.create(cols)
        this.tableEventTransformerMapper = tisUDFDefinition.map((udfs) -> new SingleTableEventTransformerMapper(cols, udfs));
    }

    /**
     * Creates an instance of {@link BinaryRecordData} with given field values.
     *
     * <p>Note: All fields of the record must be internal data structures. See {@link RecordData}.
     */
    public BinaryRecordData generate(Object[] rowFields) {
        checkArgument(
                dataTypes.length == rowFields.length,
                "The types and values must have the same length. But types is %s and values is %s",
                dataTypes.length,
                rowFields.length);

        // 执行TIS Transformer 设置值
        this.tableEventTransformerMapper.ifPresent((transformer) -> {
            try {
                transformer.map(rowFields);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });


        reuseWriter.reset();
        for (int i = 0; i < dataTypes.length; i++) {
            if (rowFields[i] == null) {
                reuseWriter.setNullAt(i);
            } else {
                try {
                    BinaryWriter.write(reuseWriter, i, rowFields[i], dataTypes[i], serializers[i]);
                } catch (Exception e) {
                    // baisui add for error debug detaild info
                    throw new RuntimeException("col:" + this.cols.get(i).name + ", index:" + i + ", dataType:" + dataTypes[i] + ", val:" + rowFields[i], e);
                }
            }
        }
        reuseWriter.complete();
        return reuseRecordData.copy();
    }
}
