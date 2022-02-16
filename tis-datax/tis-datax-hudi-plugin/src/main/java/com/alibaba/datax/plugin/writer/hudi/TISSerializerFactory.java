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

package com.alibaba.datax.plugin.writer.hudi;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsHelper;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.Serializers;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-23 20:41
 **/
public class TISSerializerFactory extends SerializerFactory {
    private final List<HdfsHelper.HdfsColMeta> colsMeta;

    public TISSerializerFactory(
            List<HdfsHelper.HdfsColMeta> colsMeta) {
        this.colsMeta = colsMeta;
    }

    @Override
    public SerializerFactory withAdditionalSerializers(Serializers additional) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SerializerFactory withAdditionalKeySerializers(Serializers additional) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SerializerFactory withSerializerModifier(BeanSerializerModifier modifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonSerializer<Object> createSerializer(
            SerializerProvider prov, JavaType baseType) throws JsonMappingException {
        //  throw new UnsupportedOperationException();
        return new TISJsonSerializer();
    }

    @Override
    public TypeSerializer createTypeSerializer(
            SerializationConfig config, JavaType baseType) throws JsonMappingException {
        //  throw new UnsupportedOperationException(baseType.toString());
        return null;
    }

    @Override
    public JsonSerializer<Object> createKeySerializer(
            SerializationConfig config, JavaType type, JsonSerializer<Object> defaultImpl) throws JsonMappingException {
        throw new UnsupportedOperationException();
    }

    private class TISJsonSerializer extends JsonSerializer<Object> {
        @Override
        public void serialize(Object value, JsonGenerator gen
                , SerializerProvider serializers) throws IOException, JsonProcessingException {
            com.alibaba.datax.common.element.Record r
                    = (com.alibaba.datax.common.element.Record) value;
            gen.writeStartObject();
            gen.setCurrentValue(r);
            int i = 0;
            Column column = null;
            for (HdfsHelper.HdfsColMeta meta : colsMeta) {

                gen.writeFieldName(meta.colName);
                column = r.getColumn(i++);

                if (column.getRawData() == null) {
                    gen.writeNull();
                    continue;
                }
                switch (meta.csvType) {
                    case STRING:
                        // gen.writeString(column.asString());
                        String content = column.asString();
                        if (StringUtils.isBlank(content)) {
                            // gen.writeString(new SerializedString(StringUtils.EMPTY));
                            gen.writeRaw("\"\"");
                        } else {
                            gen.writeString(content);
                        }
                        break;
                    case NUMBER:
                        switch (column.getType()) {
                            case STRING:
                            case BAD:
                            case BYTES:
                            case DATE:
                                gen.writeString(column.asString());
                                break;
                            case INT:
                                gen.writeNumber(column.asBigInteger());
                                break;
                            case DOUBLE:
                                gen.writeNumber(column.asDouble());
                                break;
                            case BOOL:
                                gen.writeBoolean(column.asBoolean());
                                break;
                            case LONG:
                                gen.writeNumber(column.asLong());
                                break;
                            case NULL:
                                gen.writeNull();
                                break;
                            default:
                                throw new IllegalStateException("illegal columnType:" + column.getType());
                        }
                        break;
                    case BOOLEAN:
                        gen.writeBoolean(column.asBoolean());
                        break;
                    default:
                        throw new IllegalStateException("illegal type:" + meta.csvType);
                }
            }
            // [databind#631]: Assign current value, to be accessible by custom serializers

            gen.writeEndObject();

            // throw new UnsupportedOperationException();
        }
    }
}
