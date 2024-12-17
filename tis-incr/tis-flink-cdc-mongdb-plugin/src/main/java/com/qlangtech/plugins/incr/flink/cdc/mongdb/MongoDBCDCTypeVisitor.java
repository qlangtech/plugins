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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 16:10
 * @see com.qlangtech.tis.plugin.datax.mongo.MongoDataXColUtils
 **/
public class MongoDBCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
    // private ZoneId localTimeZone;

    public MongoDBCDCTypeVisitor(IColMetaGetter meta, int colIndex) {
        super(meta, colIndex);
        //  this.localTimeZone = Objects.requireNonNull(zoneId, "zoneId can not be null");
    }

    @Override
    public FlinkCol boolType(DataType dataType) {
        return this.tinyIntType(dataType);
    }

    //    @Override
//    public FlinkCol intType(DataType type) {
//        FlinkCol col = super.intType(type);
//        return col.setSourceDTOColValProcess(new MongoInt32ValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol smallIntType(DataType dataType) {
//        FlinkCol col = super.smallIntType(dataType);
//        return col.setSourceDTOColValProcess(new MongoSmallIntValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol tinyIntType(DataType dataType) {
//        FlinkCol col = super.tinyIntType(dataType);
//        return col.setSourceDTOColValProcess(new MongoTinyIntValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol floatType(DataType type) {
//        FlinkCol col = super.floatType(type);
//        return col.setSourceDTOColValProcess(new MongoFloatValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol timeType(DataType type) {
//        FlinkCol col = super.timeType(type);
//        return col.setSourceDTOColValProcess(new MongoTimeValueDTOConvert(this.localTimeZone));
//    }
//
//    @Override
//    public FlinkCol bigInt(DataType type) {
//        FlinkCol col = super.bigInt(type);
//        return col.setSourceDTOColValProcess(new MongoBigIntDTOConvert());
//    }
//
//    @Override
//    public FlinkCol decimalType(DataType type) {
//        FlinkCol col = super.decimalType(type);
//        return col.setSourceDTOColValProcess(new MongoDecimalValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol doubleType(DataType type) {
//        FlinkCol col = super.doubleType(type);
//        return col.setSourceDTOColValProcess(new MongoDoubleDTOConvert());
//    }
//
//    @Override
//    public FlinkCol dateType(DataType type) {
//        FlinkCol col = super.dateType(type);
//        MongoDateValueDTOConvert dateValueDTOConvert = new MongoDateValueDTOConvert(this.localTimeZone);
//        return col.setSourceDTOColValProcess(dateValueDTOConvert);
//    }
//
//    @Override
//    public FlinkCol timestampType(DataType type) {
//        FlinkCol col = super.timestampType(type);
//        return col.setSourceDTOColValProcess(new MongoDateTimeValueDTOConvert(this.localTimeZone));
//    }
//
//    @Override
//    public FlinkCol bitType(DataType type) {
//        FlinkCol col = super.bitType(type);
//        return col.setSourceDTOColValProcess(new MongoBitValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol boolType(DataType dataType) {
//        FlinkCol col = super.boolType(dataType);
//        return col.setSourceDTOColValProcess(new MongoBooleanDTOConvert());
//    }
//
//    @Override
//    public FlinkCol varcharType(DataType type) {
//        FlinkCol col = super.varcharType(type);
//        return col.setSourceDTOColValProcess(new MongoStringValueDTOConvert());
//    }
//
//    @Override
//    public FlinkCol blobType(DataType type) {
//        FlinkCol col = super.blobType(type);
//        MongoBinaryRawValueDTOConvert binaryRawValueDTOConvert = new MongoBinaryRawValueDTOConvert();
//        col.setSourceDTOColValProcess(binaryRawValueDTOConvert);
//        return col;
//    }
//
//    public static class MongoBinaryRawValueDTOConvert extends BinaryRawValueDTOConvert {
//        @Override
//        public Object apply(Object o) {
//            BsonBinary bin = (BsonBinary) o;
//            return java.nio.ByteBuffer.wrap(bin.getData());
//        }
//    }
//
//    public static class MongoDateValueDTOConvert extends BiFunction {
//        private ZoneId localTimeZone;
//
//        public MongoDateValueDTOConvert(ZoneId localTimeZone) {
//            this.localTimeZone = localTimeZone;
//        }
//
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonDateTime dateTime = (org.bson.BsonDateTime) o;
//            return java.time.LocalDate.ofInstant(Instant.ofEpochMilli(dateTime.getValue()), localTimeZone);
//        }
//    }
//
//    public static class MongoDateTimeValueDTOConvert extends BiFunction {
//        private ZoneId localTimeZone;
//
//        public MongoDateTimeValueDTOConvert(ZoneId localTimeZone) {
//            this.localTimeZone = localTimeZone;
//        }
//
//        @Override
//        public Object apply(Object o) {
//            BsonValue bson = (BsonValue) o;
//            if (bson.isDateTime()) {
//                org.bson.BsonDateTime dateTime = bson.asDateTime();
//                return java.time.LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime.getValue()), localTimeZone);
//            } else if (bson.isTimestamp()) {
//                BsonTimestamp timestamp = bson.asTimestamp();
//                return java.time.LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getValue()), localTimeZone);
//            } else {
//                throw new UnsupportedOperationException("unsupported type:" + bson.getBsonType());
//            }
//        }
//    }
//
//    public static class MongoBitValueDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            throw new UnsupportedOperationException(String.valueOf(o.getClass()));
//        }
//    }
//
//    public static class MongoTimeValueDTOConvert extends MongoDateTimeValueDTOConvert {
//        public MongoTimeValueDTOConvert(ZoneId localTimeZone) {
//            super(localTimeZone);
//        }
//    }
//
//    public static class MongoBooleanDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            BsonBoolean val = (BsonBoolean) o;
//            return val.getValue();
//        }
//    }
//
//
//    public static class MongoBigIntDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            BsonInt64 val = (BsonInt64) o;
//            return val.getValue();
//        }
//    }
//
//    public static class MongoStringValueDTOConvert extends BiFunction {
//
//        @Override
//        public Object apply(Object o) {
//            BsonValue bson = (BsonValue) o;
//            if (bson.isString()) {
//                return bson.asString().getValue();
//            } else if (bson.isRegularExpression()) {
//                BsonRegularExpression regularExpression = bson.asRegularExpression();
//                return regularExpression.getPattern() + ":" + regularExpression.getOptions();
//            } else if (bson.isDocument()) {
//                BsonDocument document = bson.asDocument();
//                return document.toJson();
//            } else {
//                return bson.toString();
//            }
//
//            // throw new UnsupportedOperationException("unsupport type:" + bson.getBsonType());
//
////            BsonString val = (BsonString) o;
////            return val.getValue();
//        }
//    }
//
//    public static class MongoInt32ValueDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
//            return val.getValue();
//        }
//    }
//
//    public static class MongoDecimalValueDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonDecimal128 val = (org.bson.BsonDecimal128) o;
//            return val.getValue().bigDecimalValue();
//        }
//    }
//
//    public static class MongoDoubleDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonDouble val = (org.bson.BsonDouble) o;
//            return val.getValue();
//        }
//    }
//
//    public static class MongoSmallIntValueDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
//            return val.getValue();
//        }
//    }
//
//    public static class MongoTinyIntValueDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
//            return val.getValue();
//        }
//    }
//
//    public static class MongoFloatValueDTOConvert extends BiFunction {
//        @Override
//        public Object apply(Object o) {
//            org.bson.BsonDouble val = (org.bson.BsonDouble) o;
//            return val.getValue();
//        }
//    }


}
