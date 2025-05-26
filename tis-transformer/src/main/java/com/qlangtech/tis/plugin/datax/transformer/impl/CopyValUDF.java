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
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.ThreadCacheTableCols;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ContextParamConfig;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 用于使用虚拟列
 */
public class CopyValUDF extends AbstractFromColumnUDFDefinition {

    @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public TargetColType to;


    public static List<TargetColType> getCols() {
        return Lists.newArrayList();
    }

    private TargetColType getTO() {
        return Objects.requireNonNull(this.to, "property to can not be null");
    }

    @Override
    public List<OutputParameter> outParameters() {
        return Collections.singletonList(TargetColType.create(this.getTO()));
    }

    public static List<CMeta> fromColsCandidate() {
        List<CMeta> colsCandidate = SelectedTab.getSelectedCols();
        ThreadCacheTableCols threadCacheTabCols = SelectedTab.getContextTableColsStream();
        IDataxReader reader = threadCacheTabCols.plugin;
        Map<String, ContextParamConfig> dbContextParams = reader.getDBContextParams();
        if (MapUtils.isNotEmpty(dbContextParams)) {
            colsCandidate = Lists.newArrayList(colsCandidate);
            for (Map.Entry<String, ContextParamConfig> entry : dbContextParams.entrySet()) {
                colsCandidate.add(CMeta.create(Optional.empty(), entry.getKey(), entry.getValue().getDataType()));
            }
        }
        return colsCandidate;
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {

        record.setColumn(this.getTO().getName(), record.getColumn(this.from));
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> literia = super.getLiteria();
        literia.add(new UDFDesc(KEY_TO, this.getTO().getLiteria()));
        return literia;
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
            return "Copy Field";
        }
    }
}
