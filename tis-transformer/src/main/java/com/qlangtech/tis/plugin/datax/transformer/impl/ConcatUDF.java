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
import com.alibaba.datax.common.element.Record;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.InParamer;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 连接多个字段值，合并成一个新的
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-17 09:52
 **/
public class ConcatUDF extends UDFDefinition {

    @FormField(ordinal = 1, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<TargetColType> from;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String separator;

    @FormField(ordinal = 3, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public TargetColType to;

    @Override
    public List<InParamer> inParameters() {
        return this.from.stream().map((col) -> InParamer.create(col.getName())).collect(Collectors.toList());
    }

    /**
     * 取得可用的字段分隔符
     *
     * @return
     */
    public static List<Option> acceptableSeparator() {
        List<Option> seps = Lists.newArrayList();
        for (Separator s : Separator.values()) {
            seps.add(new Option(s.name()));
        }
        return seps;
    }

    enum Separator {
        Empty(StringUtils.EMPTY), Cut("-"), Underline("_");
        private final String sign;

        Separator(String sign) {
            this.sign = sign;
        }
    }

    public static List<TargetColType> getFromCols() {
        return Lists.newArrayList();
    }

    public static List<TargetColType> getToCols() {
        return Lists.newArrayList();
    }

    @Override
    public List<OutputParameter> outParameters() {
        return Collections.singletonList(OutputParameter.create(this.to));
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {
        record.setString(this.to.getName()
                , this.from.stream().map((f) -> record.getColumn(f.getName()))
                        //.filter((colVal) -> colVal != null)
                        .map((val) -> (val == null) ? StringUtils.EMPTY : String.valueOf(val))
                        .collect(Collectors.joining(Separator.valueOf(separator).sign)));
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> result = Lists.newArrayList();
        UDFDesc from = new UDFDesc(KEY_FROM
                , this.from.stream().flatMap((f) -> f.getLiteria().stream()).collect(Collectors.toList()));
        result.add(from);
        result.add(new UDFDesc(KEY_TO, to.getLiteria()));

        Separator sep = Separator.valueOf(separator);
        result.add(new UDFDesc("separate with", sep + "(" + sep.sign + ")"));
        return result;
    }

    @TISExtension
    public static final class DefaultDescriptor extends Descriptor<UDFDefinition> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Concat Fields";
        }
    }
}
