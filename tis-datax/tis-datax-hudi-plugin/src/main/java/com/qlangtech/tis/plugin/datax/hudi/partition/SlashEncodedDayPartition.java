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

package com.qlangtech.tis.plugin.datax.hudi.partition;

import com.alibaba.datax.plugin.writer.hudi.TypedPropertiesBuilder;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;

import java.util.List;

/**
 * // @see SlashEncodedDayPartitionValueExtractor
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-08 15:59
 **/
@Public
public class SlashEncodedDayPartition extends HudiTablePartition {

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String partitionPathField;

    @Override
    public void setProps(TypedPropertiesBuilder props, DataXHudiWriter hudiWriter) {
        super.setProps(props, hudiWriter);
        setPartitionProps(props, partitionPathField, "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor");
    }

    @Override
    public void addPartitionsOnSQLDDL(List<String> pts, CreateTableSqlBuilder createTableSqlBuilder) {
        appendPartitionsOnSQLDDL(pts, createTableSqlBuilder);
    }

    public static List<Option> getPtCandidateFields() {
        return HudiSelectedTab.getContextTableCols((cols) -> cols.stream()
                .filter((col) -> {
                    switch (col.getType().getCollapse()) {
                        // case STRING:
//                        case INT:
//                        case Long:
                        case Date:
                            return true;
                    }
                    return false;
                }));

    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<HudiTablePartition> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "slashEncodedDay";
        }
    }
}
