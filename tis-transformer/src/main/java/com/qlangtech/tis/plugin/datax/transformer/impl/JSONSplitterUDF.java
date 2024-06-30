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

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 将某列JSON内容拆解成多个字段
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-11 14:38
 **/
public class JSONSplitterUDF extends AbstractFromColumnUDFDefinition {
    private static final Logger logger = LoggerFactory.getLogger(JSONSplitterUDF.class);
    /**
     * 为拆分出来的json字段添加前缀
     */
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.identity})
    public String prefix;

    @FormField(ordinal = 3, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<TargetColType> to;

    /**
     * 是否跳过字符串解析json对象错误
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean skipError;


    @Override
    public List<OutputParameter> outParameters() {
        return this.to.stream().map((col) -> {
            return OutputParameter.create(getPrefixToFieldName(col), col);
        }).collect(Collectors.toList());//.stream().map((c) -> c).collect(Collectors.toList());
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> result = Lists.newArrayList(super.getLiteria());

        List<UDFDesc> toDesc = Lists.newArrayList();
        this.to.forEach((colType) -> {
            toDesc.addAll(colType.getLiteria());
        });
        if (StringUtils.isNotEmpty(this.prefix)) {
            toDesc.forEach((t) -> {
                t.addPair("with prefix", prefix);
            });
        }

        result.add(new UDFDesc(KEY_TO, toDesc));
        return result;
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {
        String from = this.from;
        if (CollectionUtils.isEmpty(this.to)) {
            throw new IllegalStateException("json splitter fields can not be empty");
        }

        String jsonCol = record.getString(from);
        if (jsonCol != null) {
            try {
                JSONObject json = JSON.parseObject(jsonCol);
                for (TargetColType t : this.to) {
                    Object val = json.get(t.getName());
                    if (val != null) {
                        record.setColumn(getPrefixToFieldName(t), val);
                      //  record.setColumn();
                    }
                }
            } catch (Exception e) {
                if (!this.skipError) {
                    throw new RuntimeException(e);
                } else {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    private String getPrefixToFieldName(TargetColType t) {
        return prefix == null ? t.getName() : (prefix + t.getName());
    }


    public static List<TargetColType> getCols() {
        return Lists.newArrayList();
    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<UDFDefinition> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "JSON Splitter";
        }
    }

}
