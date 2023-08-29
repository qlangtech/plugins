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

package com.qlangtech.tis.plugin.datax.seq;

import com.alibaba.datax.plugin.writer.doriswriter.Keys;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataXReaderColType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-31 15:12
 **/
public class OnSeqKey extends SeqKey {

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public String seqKey;

    @Override
    public void appendBatchCfgs(JSONObject props) {
        props.put(Keys.COL_SEQUENCE_NAME, this.seqKey);
    }

    @Override
    public boolean isOn() {
        return true;
    }

    @Override
    public String getSeqColName() {
        return this.seqKey;
    }

    @Override
    public StringBuffer createDDLScript(IDataxProcessor.TableMap tableMapper) {
        // if (StringUtils.isNotEmpty(this.seqKey)) {
        if (tableMapper == null) {
            throw new IllegalArgumentException("param tableMapper can not be null");
        }
        if (StringUtils.isEmpty(seqKey)) {
            throw new IllegalArgumentException("param seqKey can not be null");
        }
        StringBuffer seqBuffer = new StringBuffer();
        List<CMeta> cols = tableMapper.getSourceCols();
        Optional<CMeta> p = cols.stream().filter((c) -> seqKey.equals(c.getName())).findFirst();
        if (!p.isPresent()) {
            throw new IllegalStateException("can not find col:" + seqKey);
        }

//                seqBuffer.append("\n\t, \"function_column.sequence_col\" = '").append(dorisTab.seqKey)
//                        .append("'\n\t, \"function_column.sequence_type\"='").append(createColWrapper(p.get()).getMapperType()).append("'");

        seqBuffer.append("\n\t, \"function_column.sequence_col\" = '").append(seqKey).append("'");
        // .append("'\n\t, \"function_column.sequence_type\"='").append(createColWrapper(p.get()).getMapperType()).append("'");

        // }
        return seqBuffer;
    }

    /**
     * seq cols候选列
     *
     * @return
     */
    public static List<Option> getSeqKeys() {
        List<Option> result = SelectedTab.getContextTableCols((cols) -> {
            return cols.stream().filter((c) -> {
                DataXReaderColType t = c.getType().getCollapse();
                return t == DataXReaderColType.INT || t == DataXReaderColType.Long || t == DataXReaderColType.Date;
            });
        }).stream().map((c)-> new Option(c.getName())).collect(Collectors.toList());
        return result;
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<SeqKey> {
        public DftDescriptor() {
        }


        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
