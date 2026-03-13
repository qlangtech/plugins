package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.transformer;

import com.alibaba.datax.common.element.ICol2Index.Col;
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTOConvertTo;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-17 18:51
 **/
public class EventRowFields extends AbstractTransformerRecord<Object[]> {


    public EventRowFields(Object[] row, List<FlinkCol> cols) {
        super(DTOConvertTo.FlinkCDCPipelineEvent, row, cols);
    }

    @Override
    public Object getColumn(String field) {
        Col col = this.col2IndexMapper.getCol2Index().get(field);
        if (col == null) {
            throw new NullPointerException("field:" + field + " relevant col can not be null, exist cols key:"
                    + String.join(",", this.col2IndexMapper.getCol2Index().keySet()));
        }
        return this.row[col.getIndex()];
    }

//    @Override
//    public void setColumn(String field, Object colVal) {
//        if (colVal == null) {
//            return;
//        }
//        Col col = this.col2IndexMapper.getCol2Index().get(field);
//        if (col == null) {
//            throw new IllegalStateException("field:" + field + " relevant col can not be null");
//        }
//        try {
//            FlinkCol flinkCol = cols.get(col.getIndex());
//            this.row[col.getIndex()] = flinkCol.flinkCDCPipelineEventProcess.apply(colVal);
//        } catch (Exception e) {
//            throw new RuntimeException(String.valueOf(col), e);
//        }
//    }

    @Override
    protected void setColumn(String field, BiFunction rowProcess, Object colVal) {
        if (colVal == null) {
            return;
        }
        Integer pos = this.getPos(field);// this.col2IndexMapper.getCol2Index().get(field);
//        if (col == null) {
//            throw new IllegalStateException("field:" + field + " relevant col can not be null");
//        }
        try {
            // FlinkCol flinkCol = cols.get(col.getIndex());
            this.row[pos] = rowProcess.apply(colVal);
        } catch (Exception e) {
            throw new RuntimeException(field + ",pos:" + pos, e);
        }
    }

//    @Override
//    public void setString(String field, String val) {
//        this.setColumn(field, val);
//    }

//    @Override
//    public String getString(String field, boolean origin) {
//        Object colVal = this.getColumn(field);
//        return colVal != null ? String.valueOf(colVal) : null;
//    }

    @Override
    public Object[] getDelegate() {
        return this.row;
    }
}
