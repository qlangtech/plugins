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

package com.qlangtech.tis.plugin.datax.transformer.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.InParamer;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.util.IPluginContext;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 在CSV文件导入过程中很难找到一个合适的列作为主键，这时需要在导入过程中自动生成一个long类型的主键此时使用该UDF比较合适
 * 有两种id的生成方式，自增和基于雪花算法的ID生成
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-04-03 17:47
 **/
public class AutoGenerateBigIntIDUDF extends UDFDefinition implements AfterPluginSaved {

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String valType;

    @FormField(ordinal = 3, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public TargetColType to;

    private transient GenerateValueType _valType;

    private TargetColType getTO() {
        return Objects.requireNonNull(this.to, "property to can not be null");
    }

    public static List<TargetColType> getCols() {
        return Lists.newArrayList();
    }

    public static List<Option> allSupportedValTypes() {
        List<Option> types = Lists.newArrayList();
        for (GenerateValueType type : GenerateValueType.values()) {
            types.add(new Option(type.token));
        }
        return types;

    }

    @Override
    public List<OutputParameter> outParameters() {
        return Collections.singletonList(TargetColType.create(this.getTO()));
    }


    @Override
    public List<InParamer> inParameters() {
        return Collections.emptyList();
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {
        record.setColumn(this.getTO().getName(), getGenValueType().valGen.get());
    }

    private GenerateValueType getGenValueType() {
        if (this._valType == null) {
            this._valType = GenerateValueType.parse(this.valType);
        }
        return this._valType;
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this._valType = null;
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> result = Lists.newArrayList();
        result.add(new UDFDesc(KEY_TO, getTO().getLiteria()));

        final GenerateValueType valType = GenerateValueType.parse(this.valType);
        result.add(new UDFDesc("value type", valType + "(" + valType.token + ")"));
        return result;
    }

    private static final ThreadLocal<AtomicLong> atomicLongGen = new ThreadLocal<>() {
        @Override
        protected AtomicLong initialValue() {
            return new AtomicLong();
        }
    };

    public enum GenerateValueType {
        AUTO_INCR("auto_incr", () -> {
            return atomicLongGen.get().getAndIncrement();
        }), AUTO_SNOWFLAKE_ID("base_on_snowflake", () -> {
            return CodeGenerateUtils.genCode();
        });
        public final String token;
        private Supplier<Long> valGen;

        public static GenerateValueType parse(String token) {
            for (GenerateValueType type : GenerateValueType.values()) {
                if (type.token.equalsIgnoreCase(token)) {
                    return type;
                }
            }
            throw new IllegalStateException("illegal token:" + token);
        }

        private GenerateValueType(String token, Supplier<Long> valGen) {
            this.token = token;
            this.valGen = valGen;
        }
    }

    /**
     * Rewriting based on Twitter snowflake algorithm
     */
    public static class CodeGenerateUtils {

        private static final CodeGenerator codeGenerator;

        static {
            try {
                RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
                int processID = Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
                codeGenerator = new CodeGenerator(InetAddress.getLocalHost().getHostName() + "-" + processID);
            } catch (UnknownHostException e) {
                throw new CodeGenerateException(e.getMessage());
            }
        }

        public static long genCode() throws CodeGenerateException {
            return codeGenerator.genCode();
        }

        public static class CodeGenerator {

            // start timestamp
            private static final long START_TIMESTAMP = 1609430400000L; // 2021-01-01 00:00:00
            // Each machine generates 32 in the same millisecond
            private static final long LOW_DIGIT_BIT = 5L;
            private static final long MACHINE_BIT = 5L;
            private static final long MAX_LOW_DIGIT = ~(-1L << LOW_DIGIT_BIT);
            // The displacement to the left
            private static final long HIGH_DIGIT_LEFT = LOW_DIGIT_BIT + MACHINE_BIT;
            public final long machineHash;
            private long lowDigit = 0L;
            private long recordMillisecond = -1L;

            private static final long SYSTEM_TIMESTAMP = System.currentTimeMillis();
            private static final long SYSTEM_NANOTIME = System.nanoTime();

            public CodeGenerator(String appName) {
                this.machineHash = Math.abs(Objects.hash(appName)) % (1 << MACHINE_BIT);
            }

            public synchronized long genCode() throws CodeGenerateException {
                long nowtMillisecond = systemMillisecond();
                if (nowtMillisecond < recordMillisecond) {
                    throw new CodeGenerateException("New code exception because time is set back.");
                }
                if (nowtMillisecond == recordMillisecond) {
                    lowDigit = (lowDigit + 1) & MAX_LOW_DIGIT;
                    if (lowDigit == 0L) {
                        while (nowtMillisecond <= recordMillisecond) {
                            nowtMillisecond = systemMillisecond();
                        }
                    }
                } else {
                    lowDigit = 0L;
                }
                recordMillisecond = nowtMillisecond;
                return (nowtMillisecond - START_TIMESTAMP) << HIGH_DIGIT_LEFT | machineHash << LOW_DIGIT_BIT | lowDigit;
            }

            private long systemMillisecond() {
                return SYSTEM_TIMESTAMP + (System.nanoTime() - SYSTEM_NANOTIME) / 1000000;
            }
        }

        public static class CodeGenerateException extends RuntimeException {

            public CodeGenerateException(String message) {
                super(message);
            }
        }
    }

    @TISExtension
    public static final class DefaultDescriptor extends UDFDefinition.BasicUDFDesc {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.AutoGen;
        }

        @Override
        public String getDisplayName() {
            return "Create BigInt";
        }
    }

}
