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
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.mongo.FunctionWithPayload.MongoColValCreator;
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
import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/11
 */
public class MongoDataXColUtils {

    public static final Map<BsonType, MongoColValCreator> bsonTypeConvertorRegister;

    static {
        Builder<BsonType, MongoColValCreator> builder = ImmutableMap.builder();

        MongoColValCreator convertor = null;
        for (BsonType type : BsonType.values()) {
            block_switch:
            switch (type) {
                case BOOLEAN: {
                    convertor = new MongoColValCreator(new MongoBooleanDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoBooleanDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case DOUBLE: {
                    convertor = new MongoColValCreator(new MongoDoubleDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoDoubleDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case BINARY: {
                    convertor = new MongoColValCreator(new MongoBinaryRawValueDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoBinaryRawValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case ARRAY: {
                    convertor = new MongoColValCreator(new MongoArrayValueDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoArrayValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case INT32: {
                    convertor = new MongoColValCreator(new MongoInt32ValueDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoInt32ValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case INT64: {
                    convertor = new MongoColValCreator(new MongoBigIntDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoBigIntDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case TIMESTAMP:
                case DATE_TIME: {
                    convertor = new MongoColValCreator(new MongoDateTimeValueDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoDateTimeValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case DECIMAL128: {
                    convertor = new MongoColValCreator(new MongoDecimalValueDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoDecimalValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case OBJECT_ID: {
                    convertor = new MongoColValCreator(new MongoObjectIdDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoDecimalValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case NULL: {
                    convertor = new MongoColValCreator(new MongoNullDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoNullDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                case UNDEFINED:
                case DB_POINTER:
                case JAVASCRIPT:
                case END_OF_DOCUMENT:
                case REGULAR_EXPRESSION:
                case JAVASCRIPT_WITH_SCOPE:
                case SYMBOL:
                case MAX_KEY:
                case MIN_KEY:
                case DOCUMENT:
                case STRING: {
                    convertor = new MongoColValCreator(new MongoStringValueDTOConvert()) {
                        @Override
                        protected FunctionWithPayloadColumnDecorator createColumnValueCreator() {
                            return new MongoStringValueDTOConvertColumn();
                        }
                    };
                    break block_switch;
                }
                default:
                    throw new UnsupportedOperationException("unsupport type:" + type);
            }
            builder.put(type, convertor);
        }

        bsonTypeConvertorRegister = builder.build();
    }

    //  private static final ZoneId dftZone = ZoneId.systemDefault();

//    public static Column createCol(MongoCMeta cMeta, BsonValue val) {
//        return (Column) createCol(cMeta, val, true, dftZone);
//    }

    /**
     * @param cMeta
     * @param val
     * @param dataXColumType 是否取得dataXColumn类型
     * @return
     */
    public static Object createCol(MongoCMeta cMeta, BsonValue val, boolean dataXColumType, ZoneId zone) {
        BsonType mongoFieldType = cMeta.getMongoFieldType();
        BsonType bsonType = Objects.requireNonNull(val, "col :" + cMeta.getName() + " relevant val can not be null").getBsonType();

        MongoColValCreator valConvertor
                = bsonTypeConvertorRegister.get(mongoFieldType != null ? mongoFieldType : bsonType);

        if (valConvertor == null) {
            throw new IllegalStateException("col:" + cMeta.getName() + " relevant valConvertor can not be null,mongoFieldType:"
                    + mongoFieldType + ",bsonType:" + bsonType);
        }


        return dataXColumType ? valConvertor.getColumnValueCreator().create(val, zone) : valConvertor.valCreator.apply(val, zone);
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
            byte[] content = null;
            if (BsonBinarySubType.isUuid(bin.getType())) {
                content = String.valueOf(bin.asUuid()).getBytes(TisUTF8.get());
            } else if (bin.getType() == BsonBinarySubType.MD5.getValue()) {
                content = convertToMd5String(bin).getBytes(TisUTF8.get());
            } else {
                content = bin.getData();
            }
            return java.nio.ByteBuffer.wrap(content);
        }


    }

    public static class MongoBinaryRawValueDTOConvertColumn extends MongoBinaryRawValueDTOConvert implements FunctionWithPayloadColumnDecorator {
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
            MongoColValCreator convertor = null;
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < arrayVals.size(); i++) {
                val = arrayVals.get(i);
                if (val == null) {
                    continue;
                }
                convertor = bsonTypeConvertorRegister.get(val.getBsonType());
                result.append(convertor.valCreator.apply(val)).append(",");
            }
            return result.toString();
        }


    }

    public static class MongoArrayValueDTOConvertColumn extends MongoArrayValueDTOConvert implements FunctionWithPayloadColumnDecorator {
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

        protected Instant getInstant(BsonValue o) {
            org.bson.BsonDateTime dateTime = (org.bson.BsonDateTime) o;
            return Instant.ofEpochMilli(dateTime.getValue());
        }
    }

    public static class MongoDateValueDTOConvertColumn extends MongoDateValueDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DateColumn(getInstant(o).toEpochMilli());
        }
    }

    public static class MongoDateTimeValueDTOConvert implements FunctionWithPayload {

        protected Instant getInstant(BsonValue o) {
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
        }
    }

    public static class MongoDateTimeValueDTOConvertColumn
            extends MongoDateTimeValueDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DateColumn(getInstant(o).toEpochMilli());
        }
    }

    public static class MongoBitValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            throw new UnsupportedOperationException(String.valueOf(o.getClass()));
        }
    }

    public static class MongoBitValueDTOConvertColumn extends MongoBitValueDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            throw new UnsupportedOperationException(String.valueOf(o.getClass()));
        }
    }

    public static class MongoTimeValueDTOConvert extends MongoDateTimeValueDTOConvert {

    }

    public static class MongoBooleanDTOConvert implements FunctionWithPayload {


        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonBoolean val = (BsonBoolean) o;
            return val.getValue();
        }
    }

    public static class MongoBooleanDTOConvertColumn extends MongoBooleanDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new BoolColumn((Boolean) apply(o, payloads));
        }
    }


    public static class MongoBigIntDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            BsonInt64 val = (BsonInt64) o;
            return val.getValue();
        }
    }

    public static class MongoBigIntDTOConvertColumn extends MongoBigIntDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new LongColumn((Long) apply(o, payloads));
        }
    }

    public static class MongoStringValueDTOConvert implements FunctionWithPayload {

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

    public static class MongoStringValueDTOConvertColumn
            extends MongoStringValueDTOConvert implements FunctionWithPayloadColumnDecorator {

        @Override
        public Column create(BsonValue o, Object... payloads) {
            Object value = apply(o, payloads);
            return new StringColumn((String) value);
        }
    }

    public static class MongoInt32ValueDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonInt32 val = (org.bson.BsonInt32) o;
            return val.getValue();
        }


    }

    public static class MongoInt32ValueDTOConvertColumn extends MongoInt32ValueDTOConvert implements FunctionWithPayloadColumnDecorator {
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


    }

    public static class MongoDecimalValueDTOConvertColumn extends MongoDecimalValueDTOConvert implements FunctionWithPayloadColumnDecorator {
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
    }

    public static class MongoObjectIdDTOConvertColumn extends MongoObjectIdDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new StringColumn((String) apply(o, payloads));
        }
    }

    public static class MongoNullDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonNull nullVal = (org.bson.BsonNull) o;
            return null;
        }


    }

    public static class MongoNullDTOConvertColumn extends MongoNullDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return Column.NULL;
        }
    }


    public static class MongoDoubleDTOConvert implements FunctionWithPayload {
        @Override
        public Object apply(BsonValue o, Object... payloads) {
            org.bson.BsonDouble val = (org.bson.BsonDouble) o;
            return val.getValue();
        }
    }

    public static class MongoDoubleDTOConvertColumn extends MongoDoubleDTOConvert implements FunctionWithPayloadColumnDecorator {
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
    }

    public static class MongoSmallIntValueDTOConvertColumn
            extends MongoSmallIntValueDTOConvert implements FunctionWithPayloadColumnDecorator {
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


    }

    public static class MongoTinyIntValueDTOConvertColumn extends MongoTinyIntValueDTOConvert implements FunctionWithPayloadColumnDecorator {
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
    }

    public static class MongoFloatValueDTOConvertColumn extends MongoFloatValueDTOConvert implements FunctionWithPayloadColumnDecorator {
        @Override
        public Column create(BsonValue o, Object... payloads) {
            return new DoubleColumn((Double) apply(o, payloads));
        }
    }
}
