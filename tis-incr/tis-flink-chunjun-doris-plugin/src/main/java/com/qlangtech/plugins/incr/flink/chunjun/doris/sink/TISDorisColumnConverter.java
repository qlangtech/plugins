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

package com.qlangtech.plugins.incr.flink.chunjun.doris.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.doris.options.DorisConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugin.datax.BasicDorisStarRocksWriter;
import com.qlangtech.tis.plugin.ds.DataType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.table.data.RowData;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-18 11:03
 **/
public class TISDorisColumnConverter
        extends AbstractRowConverter<RowData, RowData, List<String>, BasicDorisStarRocksWriter.DorisType> {

    private List<String> fullColumn;
    private List<String> columnNames;
    private final DorisConf options;

    private static final String NULL_VALUE = "\\N";

    private final Map<String, Integer> col2ordMap;
    // private final Map<String, ISerializationConverter> col2ExternalConverterMap;

    public TISDorisColumnConverter(DorisConf options) {
        super(options.getColumn().size());
        this.options = options;
        FieldConf col = null;
        BasicDorisStarRocksWriter.DorisType dorisType = null;

        this.col2ordMap = Maps.newHashMap();
        //  this.col2ExternalConverterMap = Maps.newHashMap();
        ISerializationConverter extrnalColConerter = null;
        for (int i = 0; i < options.getColumn().size(); i++) {
            col = options.getColumn().get(i);
            dorisType = col.getType();
            this.col2ordMap.put(col.getName(), i);
            extrnalColConerter = wrapIntoNullableExternalConverter(createExternalConverter(dorisType), dorisType);
            toExternalConverters.add(extrnalColConerter);
            //  this.col2ExternalConverterMap.put(col.getName(), extrnalColConerter);
        }
    }


    @Override
    public List<ColVal> getValByColName(RowData value, List<String> cols) {
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalArgumentException("param cols can not be empty");
        }
        try {
            List<ColVal> result = Lists.newArrayList();
            Integer ord = null;
            List<String> val = new ArrayList<>(1);
            for (String col : cols) {
                ord = col2ordMap.get(col);
                val.clear();
                toExternalConverters.get(ord).serialize(value, ord, val);
                result.add(new ColVal(col, val.get(0)));
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public RowData toInternal(RowData input) {
        return null;
    }

    @Override
    public List<String> toExternal(RowData rowData, List<String> joiner) throws Exception {
        if (fullColumn.size() == options.getColumn().size()) {
            for (int index = 0; index < rowData.getArity(); index++) {
                toExternalConverters.get(index).serialize(rowData, index, joiner);
            }
        } else {
            for (String columnName : fullColumn) {
                if (columnNames.contains(columnName)) {
                    int index = columnNames.indexOf(columnName);
                    toExternalConverters.get(index).serialize(rowData, index, joiner);
                } else {
                    joiner.add(NULL_VALUE);
                }
            }
        }
        return joiner;
    }

    @Override
    protected ISerializationConverter<List<String>> wrapIntoNullableExternalConverter(
            ISerializationConverter<List<String>> serializeConverter, BasicDorisStarRocksWriter.DorisType type) {
        return ((rowData, index, joiner) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                joiner.add(NULL_VALUE);
            } else {
                serializeConverter.serialize(rowData, index, joiner);
            }
        });
    }

    @Override
    protected ISerializationConverter<List<String>> createExternalConverter(final BasicDorisStarRocksWriter.DorisType type) {
       final  BiFunction dateProcess = FlinkCol.LocalDate();
        return (rowData, index, joiner) -> {

            Object val = (rowData.isNullAt(index)) ? null : type.type.accept(new DataType.TypeVisitor<Object>() {
                @Override
                public Object bigInt(DataType type) {
                    return (rowData.getLong(index));
                }

                @Override
                public Object doubleType(DataType type) {
                    return (rowData.getDouble(index));
                }

                @Override
                public Object dateType(DataType type) {
                   // dateProcess.deApply()
                    LocalDate localDate = LocalDate.ofEpochDay(rowData.getInt(index));
                    return dateProcess.deApply(localDate);
                }

                @Override
                public Object timestampType(DataType type) {
                    return (rowData.getTimestamp(index, -1));
                }

                @Override
                public Object bitType(DataType type) {
                    return (rowData.getInt(index));
                }

                @Override
                public Object blobType(DataType type) {
                    return new String(rowData.getBinary(index));
                }

                @Override
                public Object varcharType(DataType type) {
                    return rowData.getString(index).toString();
                }

                @Override
                public Object intType(DataType type) {
                    return (rowData.getInt(index));
                }

                @Override
                public Object floatType(DataType type) {
                    return (rowData.getFloat(index));
                }

                @Override
                public Object decimalType(DataType type) {
                    return rowData.getDecimal(index, -1, -1);
                }

                @Override
                public Object timeType(DataType type) {
                    return rowData.getInt(index);
                }

                @Override
                public Object tinyIntType(DataType dataType) {
                    return rowData.getShort(index);
                }

                @Override
                public Object smallIntType(DataType dataType) {
                    return rowData.getShort(index);
                }
            });

            // AbstractBaseColumn value = ((ColumnRowData) rowData).getField(index);
            joiner.add(
                    val == null ? NULL_VALUE : String.valueOf(val));
        };
    }

    public void setFullColumn(List<String> fullColumn) {
        this.fullColumn = fullColumn;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

}
