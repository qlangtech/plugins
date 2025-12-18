package com.qlangtech.tis.plugin.datax.transformer.impl;

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.InParamer;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 用户执行一个常量值拷贝到目标列
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/18
 */
public class CopyConstantValUDF extends UDFDefinition {

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.none_blank})
    public String from;


    @FormField(ordinal = 2, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public TargetColType to;

    @Override
    public List<OutputParameter> outParameters() {
        return Collections.singletonList(TargetColType.create(this.getTO()));
    }

    private TargetColType getTO() {
        return Objects.requireNonNull(this.to, "property to can not be null");
    }

    @Override
    public final List<InParamer> inParameters() {
        return Collections.emptyList();
    }

    @Override
    public List<UDFDesc> getLiteria() {
        return Lists.newArrayList(new UDFDesc(KEY_FROM, this.from), new UDFDesc(KEY_TO, this.getTO().getLiteria()));
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {
        record.setColumn(this.getTO().getName(), this.from);
    }


    @TISExtension
    public static class DefaultDescriptor extends UDFDefinition.BasicUDFDesc {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.Copy;
        }


        @Override
        public String getDisplayName() {
            return "Copy Constant";
        }
    }
}
