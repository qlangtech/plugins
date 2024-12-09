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

package com.qlangtech.plugins.incr.flink.cdc;

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-29 09:41
 **/
public class ColMeta {
    final int statementIndex;
    final HdfsColMeta meta;

    public ColMeta(int statementIndex, HdfsColMeta meta) {
        this.statementIndex = statementIndex;
        this.meta = meta;
    }

    @Override
    public String toString() {
        return "idx:" + this.statementIndex + ",name:" + this.meta.getName() + ",type:" + this.meta.getType().getTypeDesc() + ",pk:" + this.meta.pk;
    }

    public DataType getType() {
        return this.meta.type;
    }

    public String getName() {
        return meta.getName();
    }


    public void setTestVal(PreparedStatement statement, TestRow r) {
        this.setTestVal(IStatementSetter.create(statement), r);
    }


    public void setTestVal(IStatementSetter statement, TestRow r) {


        if (r.vals.getV(getName()) == null) {
            try {
                statement.setNull(this, getType().getType());
            } catch (Exception e) {
                throw new RuntimeException("colName:" + getName(), e);
            }
            return;
        }


        meta.type.accept(new DataType.TypeVisitor<Void>() {
            @Override
            public Void bigInt(DataType type) {
                try {
                    statement.setLong(ColMeta.this, (Long) r.getObj(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void doubleType(DataType type) {
                try {
                    statement.setDouble(ColMeta.this, (Double) r.getObj(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void dateType(DataType type) {
                try {
                    //    java.sql.Timestamp
                    java.util.Date date = (java.util.Date) r.getObj(getName());

                    statement.setDate(ColMeta.this, (new java.sql.Date(date.getTime())));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void timestampType(DataType type) {
                Object val = r.getObj(getName());
                if (val instanceof java.sql.Date) {
                    return dateType(type);
                }
                try {
                    statement.setTimestamp(ColMeta.this, (java.sql.Timestamp) val);
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void bitType(DataType type) {
                try {
                    statement.setBoolean(ColMeta.this, (Boolean) r.getObj(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void blobType(DataType type) {
                try {
                    Object val = r.getObj(getName());
                    byte[] byteVal = null;
                    if (val instanceof ByteArrayInputStream) {
                        byteVal = IOUtils.toByteArray((ByteArrayInputStream) val);
                    } else if (val instanceof Boolean) {
                        byteVal = new byte[]{(byte) (((Boolean) val) ? 1 : 0)};
                    } else {
                        byteVal = (byte[]) val;
                    }

                    if ("boolean".equalsIgnoreCase(type.typeName)) {
                        statement.setByte(ColMeta.this, byteVal[0]);
                    } else {
                        statement.setBytes(ColMeta.this, byteVal);
                    }

                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void varcharType(DataType type) {
                try {
                    statement.setString(ColMeta.this, r.getString(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void intType(DataType type) {
                try {
                    statement.setInt(ColMeta.this, r.getInt(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void floatType(DataType type) {
                try {
                    statement.setFloat(ColMeta.this, (Float) r.getObj(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void decimalType(DataType type) {
                try {
                    statement.setBigDecimal(ColMeta.this, r.getBigDecimal(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void timeType(DataType type) {
                try {
                    statement.setTime(ColMeta.this, (java.sql.Time) r.getObj(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void tinyIntType(DataType dataType) {
                try {
                    statement.setByte(ColMeta.this, (byte) r.getObj(getName()));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName(), e);
                }
                return null;
            }

            @Override
            public Void smallIntType(DataType dataType) {
                Object val = r.getObj(getName());
                if (val instanceof Byte) {
                    val = ((Byte) val).shortValue();
                }
                try {
                    statement.setShort(ColMeta.this, (Short) val);
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + getName() + ",val:" + val, e);
                }
                return null;
            }
        });


    }
}
