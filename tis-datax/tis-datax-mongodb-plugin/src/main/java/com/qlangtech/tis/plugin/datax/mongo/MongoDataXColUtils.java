package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/11
 */
public class MongoDataXColUtils {

    public static final Map<BsonType, FunctionWithPayload> bsonTypeConvertorRegister;

    static {
        Builder<BsonType, FunctionWithPayload> builder = ImmutableMap.builder();

        FunctionWithPayload convertor = null;
        for (BsonType type : BsonType.values()) {
            block_switch:
            switch (type) {
                case BOOLEAN: {
                    convertor = new MongoBooleanDTOConvert();
                    break block_switch;
                }
                case DOUBLE: {
                    convertor = new MongoDoubleDTOConvert();
                    break block_switch;
                }
                case BINARY: {
                    convertor = new MongoBinaryRawValueDTOConvert();
                    break block_switch;
                }
                case ARRAY: {
                    convertor = new MongoArrayValueDTOConvert();
                    break block_switch;
                }
                case INT32: {
                    convertor = new MongoInt32ValueDTOConvert();
                    break block_switch;
                }
                case INT64: {
                    convertor = new MongoBigIntDTOConvert();
                    break block_switch;
                }
                case TIMESTAMP:
                case DATE_TIME: {
                    convertor = new MongoDateTimeValueDTOConvert();
                    break block_switch;
                }
                case DECIMAL128: {
                    convertor = new MongoDecimalValueDTOConvert();
                    break block_switch;
                }
                case OBJECT_ID: {
                    convertor = new MongoObjectIdDTOConvert();
                    break block_switch;
                }
                case UNDEFINED:
                case DB_POINTER:
                case JAVASCRIPT:
                case END_OF_DOCUMENT:
                case REGULAR_EXPRESSION:
                case JAVASCRIPT_WITH_SCOPE:
                case NULL:
                case SYMBOL:
                case MAX_KEY:
                case MIN_KEY:
                case DOCUMENT:
                case STRING: {
                    convertor = new MongoStringValueDTOConvert();
                    break block_switch;
                }
                default:
                    throw new UnsupportedOperationException("unsupport type:" + type);
            }
            builder.put(type, convertor);
        }

        bsonTypeConvertorRegister = builder.build();
    }

    private static final ZoneId dftZone = ZoneId.systemDefault();

    public static Column createCol(MongoCMeta cMeta, BsonValue val) {
        BsonType mongoFieldType = cMeta.getMongoFieldType();
        BsonType bsonType = val.getBsonType();

        FunctionWithPayload valConvertor
                = bsonTypeConvertorRegister.get(mongoFieldType != null ? mongoFieldType : bsonType);

        if (valConvertor == null) {
            throw new IllegalStateException("col:" + cMeta.getName() + " relevant valConvertor can not be null,mongoFieldType:"
                    + mongoFieldType + ",bsonType:" + bsonType);
        }

        return valConvertor.create(val, dftZone);
        // cMeta.getMongoFieldType()

//        if (val == null) {
//            //continue; 这个不能直接continue会导致record到目的端错位
//            return new StringColumn(null);
//        } else if (val instanceof Double) {
//            //TODO deal with Double.isNaN()
//            return new DoubleColumn((Double) val);
//        } else if (val instanceof Boolean) {
//            return (new BoolColumn((Boolean) val));
//        } else if (val instanceof Date) {
//            return (new DateColumn((Date) val));
//        } else if (val instanceof Integer) {
//            return (new LongColumn((Integer) val));
//        } else if (val instanceof Long) {
//            return (new LongColumn((Long) val));
//        } else {
//
//            if (cMeta.getMongoFieldType() == BsonType.ARRAY) {
//                List array = (List) val;
//                Stream<String> stream = array.stream().filter((o) -> o != null).map((o) -> String.valueOf(o));
//                return (new StringColumn(stream.collect(Collectors.joining(","))));
//            } else {
//                return (new StringColumn(val.toString()));
//            }
//        }
    }

    public static class MongoBinaryRawValueDTOConvert implements FunctionWithPayload {

        /**
         * 将 BsonBinary 实例转换为 MD5 字符串格式
         *
         * @param bsonBinary BsonBinary 对象
         * @return 对应的 MD5 字符串
         */
        private static String convertToMd5String(BsonBinary bsonBinary) {
            if (bsonBinary == null || bsonBinary.getType() != BsonBinarySubType.MD5.getValue()) {
                throw new IllegalArgumentException("The provided BsonBinary is not of type MD5.");
            }

            byte[] data = bsonBinary.getData();
            if (data.length != 16) {
                throw new IllegalArgumentException("Invalid MD5 data length. Expected 16 bytes, but got " + data.length + " bytes.");
            }

            StringBuilder hexString = new StringBuilder();
            for (byte b : data) {
                String hex = Integer.toHexString(0xFF & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return hexString.toString();
        }

        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonBinary bin = (BsonBinary) o;
            if (BsonBinarySubType.isUuid(bin.getType())) {
                return String.valueOf(bin.asUuid());
            } else if (bin.getType() == BsonBinarySubType.MD5.getValue()) {
                return convertToMd5String(bin);
            }
            return java.nio.ByteBuffer.wrap(bin.getData());
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            Object val = apply(o, payloads);
            if (val instanceof String) {
                return new StringColumn((String) val);
            } else if (val instanceof ByteBuffer) {
                return new BytesColumn(((ByteBuffer) val).array());
            } else {
                throw new IllegalStateException("illegal type:" + val.getClass().getSimpleName());
            }
        }
    }

    public static class MongoArrayValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonArray arrayVals = (BsonArray) o;
            BsonValue val = null;
            FunctionWithPayload convertor = null;
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < arrayVals.size(); i++) {
                val = arrayVals.get(i);
                if (val == null) {
                    continue;
                }
                convertor = bsonTypeConvertorRegister.get(val.getBsonType());
                result.append(convertor.apply(val)).append(",");
            }
            return result.toString();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new StringColumn((String) apply(o, payloads));
        }
    }

    private static ZoneId getZoneId(Object[] payloads) {
        if (payloads.length < 1) {
            throw new IllegalArgumentException("payloads size can not small than 1");
        }
        return ZoneId.class.cast(payloads[0]);
    }

    public static class MongoDateValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            ZoneId localTimeZone = getZoneId(payloads);
            return java.time.LocalDate.ofInstant(getInstant(o), localTimeZone);
        }

        private Instant getInstant(BsonValue o) {
            org.bson.BsonDateTime dateTime = (org.bson.BsonDateTime) o;
            return Instant.ofEpochMilli(dateTime.getValue());
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DateColumn(getInstant(o).toEpochMilli());
        }
    }

    public static class MongoDateTimeValueDTOConvert implements FunctionWithPayload {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DateColumn(getInstant(o).toEpochMilli());
        }

        private Instant getInstant(BsonValue o) {
            BsonValue bson = o;
            if (bson.isDateTime()) {
                org.bson.BsonDateTime dateTime = bson.asDateTime();
                return Instant.ofEpochMilli(dateTime.getValue());
            } else if (bson.isTimestamp()) {
                BsonTimestamp timestamp = bson.asTimestamp();
                return Instant.ofEpochSecond(timestamp.getTime());
            } else {
                throw new UnsupportedOperationException("unsupported type:" + bson.getBsonType());
            }
        }

        @Override
        public Object apply(BsonValue o, Object... payloads) {
            ZoneId localTimeZone = getZoneId(payloads);
            Instant instant = getInstant(o);
            return java.time.LocalDateTime.ofInstant(instant, localTimeZone);

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
        }
    }

    public static class MongoBitValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            throw new UnsupportedOperationException(String.valueOf(o.getClass()));
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            throw new UnsupportedOperationException(String.valueOf(o.getClass()));
        }
    }

    public static class MongoTimeValueDTOConvert extends MongoDateTimeValueDTOConvert {

    }

    public static class MongoBooleanDTOConvert implements FunctionWithPayload {

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new BoolColumn((Boolean) apply(o, payloads));
        }

        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonBoolean val = (BsonBoolean) o;
            return val.getValue();
        }
    }


    public static class MongoBigIntDTOConvert implements FunctionWithPayload {

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new LongColumn((Long) apply(o, payloads));
        }

        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonInt64 val = (BsonInt64) o;
            return val.getValue();
        }
    }

    public static class MongoStringValueDTOConvert implements FunctionWithPayload {

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new StringColumn((String) apply(o, payloads));
        }

        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonValue bson = o;
            if (bson.isString()) {
                return bson.asString().getValue();
            } else if (bson.isRegularExpression()) {
                BsonRegularExpression regularExpression = bson.asRegularExpression();
                return regularExpression.getPattern() + ":" + regularExpression.getOptions();
            } else if (bson.isDocument()) {
                BsonDocument document = bson.asDocument();
                return document.toJson();
            } else {
                return bson.toString();
            }
        }
    }

    public static class MongoInt32ValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
            return val.getValue();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new LongColumn((Integer) apply(o, payloads));
        }
    }

    public static class MongoDecimalValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonDecimal128 val = (org.bson.BsonDecimal128) o;
            return val.getValue().bigDecimalValue();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DoubleColumn((BigDecimal) apply(o, payloads));
        }
    }

    public static class MongoObjectIdDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonObjectId val = (org.bson.BsonObjectId) o;
            return val.getValue().toHexString();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new StringColumn((String) apply(o, payloads));
        }
    }

    public static class MongoDoubleDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonDouble val = (org.bson.BsonDouble) o;
            return val.getValue();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DoubleColumn((Double) apply(o, payloads));
        }
    }

    public static class MongoSmallIntValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
            return val.getValue();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new LongColumn((Integer) apply(o, payloads));
        }
    }

    public static class MongoTinyIntValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
            return val.getValue();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new LongColumn((Integer) apply(o, payloads));
        }
    }

    public static class MongoFloatValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonDouble val = (org.bson.BsonDouble) o;
            return val.getValue();
        }

        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DoubleColumn((Double) apply(o, payloads));
        }
    }
}
