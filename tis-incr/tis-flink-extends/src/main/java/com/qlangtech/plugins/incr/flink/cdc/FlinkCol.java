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

import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-17 16:11
 **/
public class FlinkCol implements Serializable {
    public final String name;
    public final DataType type;

    private boolean pk;

    public BiFunction process;

    public FlinkCol(String name, DataType type
    ) {
        this(name, type, new NoOpProcess());
    }

    public FlinkCol(String name, DataType type, BiFunction process) {
        this.name = name;
        this.type = type;
        this.process = process;
    }

    public boolean isPk() {
        return pk;
    }

    public FlinkCol setPk(boolean pk) {
        this.pk = pk;
        return this;
    }

    public Object processVal(Object val) {
        return this.process.apply(val);
    }

    public static BiFunction Bytes() {
        return new ByteBufferProcess();
    }

    public static BiFunction DateTime() {
        return new DateTimeProcess();
    }

    public static BiFunction Date() {
        return new DateProcess();
    }

    public static BiFunction NoOp() {
        return new NoOpProcess();
    }

    private static class ByteBufferProcess extends BiFunction {
        @Override
        public Object apply(Object o) {
            java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) o;
            return buffer.array();
        }

        @Override
        public Object deApply(Object o) {
            return null;
        }
    }

    private static class DateProcess extends BiFunction {
        private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        @Override
        public Object apply(Object o) {
            if (o instanceof String) {
                // com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter
                return LocalDate.parse((String) o, dateFormatter);
            }
            return (LocalDate) o;
        }

        @Override
        public Object deApply(Object o) {
            return dateFormatter.format((LocalDate) o);
        }
    }

    private static class DateTimeProcess extends BiFunction {
        private final static DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        @Override
        public Object deApply(Object o) {
            return datetimeFormatter.format((LocalDateTime) o);
        }

        @Override
        public Object apply(Object o) {
            if (o instanceof String) {
                // com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter
                return LocalDateTime.parse((String) o, datetimeFormatter);
            }
            return (LocalDateTime) o;
        }
    }

    private static class NoOpProcess extends BiFunction {
        @Override
        public Object apply(Object o) {
            return o;
        }

        @Override
        public Object deApply(Object o) {
            return o;
        }
    }
}
