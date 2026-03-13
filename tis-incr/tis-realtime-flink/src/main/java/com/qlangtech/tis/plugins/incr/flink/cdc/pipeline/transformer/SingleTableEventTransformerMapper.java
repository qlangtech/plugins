package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.transformer;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
import com.qlangtech.tis.plugins.incr.flink.cdc.ReocrdTransformerMapper;

import java.util.List;

/**
 * 针对单个表的Event Transformer处理器
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 10:49
 **/
public class SingleTableEventTransformerMapper extends ReocrdTransformerMapper<Object[]> {

    public SingleTableEventTransformerMapper(List<FlinkCol> cols, List<UDFDefinition> transformerUDF) {
        super(cols, transformerUDF);
    }

    @Override
    protected AbstractTransformerRecord<Object[]> createDelegate(Object[] row) {
        return new EventRowFields(row, this.cols);
    }
}
