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

package com.qlangtech.tis.plugin.datax.doris;

import com.alibaba.datax.plugin.writer.doriswriter.DorisWriterKeys;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable.BasicDescriptor;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 09:39
 * // @see com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriter
 * @see com.alibaba.datax.plugin.writer.doriswriter.DorisWriter
 **/
@Public
public class DataXDorisWriter extends BasicDorisWriter {


    public static List<BasicDescriptor> autoCreateTableFilter(List<BasicDescriptor> descs, String endType) {
        if (CollectionUtils.isEmpty(descs)) {
            return Collections.emptyList();
        }
        EndType targetEndType = EndType.parse(endType);
        return descs.stream().filter((desc) -> {
            return desc.getEndType() == null || targetEndType == desc.getEndType();
        }).collect(Collectors.toList());
    }

    public static String getDftLoadProps() {
        return "{\n" +
                "    \"" + DorisWriterKeys.LOAD_PROPS_COLUMN_SEPARATOR + "\": \"" + Separator.COL_SEPARATOR_DEFAULT + "\",\n" +
                "    \"" + DorisWriterKeys.LOAD_PROPS_LINE_DELIMITER + "\": \"" + Separator.ROW_DELIMITER_DEFAULT + "\"\n" +
                "}";
    }

//    @Override
//    public boolean isGenerateCreateDDLSwitchOff() {
//        return !this.autoCreateTable.enabled();
//    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        return new DorisWriterContext(this, tableMap.get(), transformerRules);
    }

    @Override
    public Separator getSeparator() {
        JSONObject props = getLoadProps();
        return new Separator() {
            @Override
            public String getColumnSeparator() {
                return StringUtils.defaultIfBlank(props.getString(DorisWriterKeys.LOAD_PROPS_COLUMN_SEPARATOR), Separator.COL_SEPARATOR_DEFAULT);
            }

            @Override
            public String getRowDelimiter() {
                return StringUtils.defaultIfBlank(props.getString(DorisWriterKeys.LOAD_PROPS_LINE_DELIMITER), Separator.ROW_DELIMITER_DEFAULT);
            }
        };
    }

    public static final DataType.TypeVisitor<DorisType> columnTokenRecognise = new ColumnTokenRecognise() {
        @Override
        protected String getDecimalToken() {
            return "DECIMALV3";
        }
    };

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDorisWriter.class, "writer-tpl.json");
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor implements DataxWriter.IRewriteSuFormProperties {

        public DefaultDescriptor() {
            super();
        }

        @Override
        public Descriptor<SelectedTab> getRewriterSelectTabDescriptor() {
            Class targetClass = DorisSelectedTab.class;
            return Objects.requireNonNull(TIS.get().getDescriptor(targetClass)
                    , "subForm clazz:" + targetClass + " can not find relevant Descriptor");
        }

        @Override
        public String getDisplayName() {
            return DorisSourceFactory.NAME_DORIS;
        }

        @Override
        protected String getRowDelimiterKey() {
            return DorisWriterKeys.LOAD_PROPS_LINE_DELIMITER;
        }

        @Override
        protected String getColSeparatorKey() {
            return DorisWriterKeys.LOAD_PROPS_COLUMN_SEPARATOR;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return IEndTypeGetter.EndType.Doris;
        }
    }
}
