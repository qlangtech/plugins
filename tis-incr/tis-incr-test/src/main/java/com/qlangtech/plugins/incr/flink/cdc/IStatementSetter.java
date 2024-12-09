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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-06 09:03
 **/
public interface IStatementSetter {

    public static IStatementSetter create(PreparedStatement ps) {
        return new IStatementSetter() {
            @Override
            public void setNull(ColMeta colMeta, int type) throws Exception {
                ps.setNull(colMeta.statementIndex, type);
            }

            @Override
            public void setLong(ColMeta colMeta, Long obj) throws Exception {
                ps.setLong(colMeta.statementIndex, obj);
            }

            @Override
            public void setDouble(ColMeta colMeta, Double obj) throws Exception {
                ps.setDouble(colMeta.statementIndex, obj);
            }

            @Override
            public void setDate(ColMeta colMeta, Date obj) throws Exception {
                ps.setDate(colMeta.statementIndex, obj);
            }

            @Override
            public void setTimestamp(ColMeta colMeta, Timestamp val) throws Exception {
                ps.setTimestamp(colMeta.statementIndex, val);
            }

            @Override
            public void setBoolean(ColMeta colMeta, Boolean obj) throws Exception {
                ps.setBoolean(colMeta.statementIndex, obj);
            }

            @Override
            public void setByte(ColMeta colMeta, byte b) throws Exception {
                ps.setByte(colMeta.statementIndex, b);
            }

            @Override
            public void setBytes(ColMeta colMeta, byte[] byteVal) throws Exception {
                ps.setBytes(colMeta.statementIndex, byteVal);
            }

            @Override
            public void setString(ColMeta colMeta, String val) throws Exception {
                ps.setString(colMeta.statementIndex, val);
            }

            @Override
            public void setInt(ColMeta colMeta, Integer val) throws Exception {
                ps.setInt(colMeta.statementIndex, val);
            }

            @Override
            public void setFloat(ColMeta colMeta, Float obj) throws Exception {
                ps.setFloat(colMeta.statementIndex, obj);
            }

            @Override
            public void setBigDecimal(ColMeta colMeta, BigDecimal bigDecimal) throws Exception {
                ps.setBigDecimal(colMeta.statementIndex, bigDecimal);
            }

            @Override
            public void setTime(ColMeta colMeta, Time obj) throws Exception {
                ps.setTime(colMeta.statementIndex, obj);
            }

            @Override
            public void setShort(ColMeta colMeta, Short val) throws Exception {
                ps.setShort(colMeta.statementIndex, val);
            }
        };
    }

    void setNull(ColMeta colMeta, int type) throws Exception;


    void setLong(ColMeta colMeta, Long obj) throws Exception;

    void setDouble(ColMeta colMeta, Double obj) throws Exception;

    void setDate(ColMeta colMeta, Date obj) throws Exception;

    void setTimestamp(ColMeta colMeta, Timestamp val) throws Exception;

    void setBoolean(ColMeta colMeta, Boolean obj) throws Exception;

    void setByte(ColMeta colMeta, byte b) throws Exception;

    void setBytes(ColMeta colMeta, byte[] byteVal) throws Exception;

    void setString(ColMeta colMeta, String string) throws Exception;

    void setInt(ColMeta colMeta, Integer anInt) throws Exception;

    void setFloat(ColMeta colMeta, Float obj) throws Exception;

    void setBigDecimal(ColMeta colMeta, BigDecimal bigDecimal) throws Exception;

    void setTime(ColMeta colMeta, Time obj) throws Exception;

    void setShort(ColMeta colMeta, Short val) throws Exception;
}
