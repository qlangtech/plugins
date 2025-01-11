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

import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.common.utils.Assert;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules.TransformerOverwriteCols;
import com.qlangtech.tis.plugin.datax.transformer.TargetColumn;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-11 14:51
 **/
public class TestJSONSplitterUDF extends BasicUDFDefinitionTest<JSONSplitterUDF> {
    static final String output_param_col1 = "col1_smallint";
    static final String output_param_col2 = "col2_smallint";
    static final String output_param_col3 = "col3_varchar";
    static final String key_col_ext = "ext";

    static final String prefix = "json_";

    @Override
    protected Class<JSONSplitterUDF> getPluginClass() {
        return JSONSplitterUDF.class;
    }

    @Override
    protected JSONSplitterUDF createTransformerUDF() {
        JSONSplitterUDF udf = new JSONSplitterUDF();
        udf.prefix = prefix;
        udf.skipError = false;
        udf.from = "ext";
        List<TargetColType> toCols = Lists.newArrayList();
        TargetColType colType = new TargetColType();
        colType.setType(DataType.getType(JDBCTypes.SMALLINT));
        TargetColumn col = TargetColumn.create(true, output_param_col1);
        colType.setTarget(col);
        toCols.add(colType);


        colType = new TargetColType();
        colType.setType(DataType.getType(JDBCTypes.SMALLINT));
        col = TargetColumn.create(true, output_param_col2);
        colType.setTarget(col);
        toCols.add(colType);

        colType = new TargetColType();
        colType.setType(DataType.createVarChar(32));
        col = TargetColumn.create(true, output_param_col3);
        colType.setTarget(col);
        toCols.add(colType);

        udf.to = toCols;

        return udf;
    }

    @Test
    public void testEvaluate() {

        RunningContext runningContext = mock("runningContext", RunningContext.class);
        ITransformerBuildInfo transformerBuildInfo = mock("transformerBuildInfo", ITransformerBuildInfo.class);

        List<SelectedTab> selectedTabs = TestSelectedTabs.createSelectedTabs(1, (tab) -> {
            tab.cols.add(CMeta.create(key_col_ext, JDBCTypes.VARCHAR));
        });

        SelectedTab tab = selectedTabs.get(0);

        JSONSplitterUDF jsonSplitterUDF = createTransformerUDF();

        RecordTransformerRules transformerRules = RecordTransformerRules.create(jsonSplitterUDF);
        TransformerOverwriteCols<OutputParameter> overwriteCols = transformerRules.overwriteCols(tab.getCols());

        EasyMock.expect(transformerBuildInfo.containContextParams()).andReturn(false);
        EasyMock.expect(transformerBuildInfo.overwriteColsWithContextParams(matchCols(tab))).andReturn(overwriteCols);
        EasyMock.expect(transformerBuildInfo.tranformerColsWithoutContextParams()).andReturn(overwriteCols);

        DefaultRecord record = new DefaultRecord();
//        Map<String, ColumnBiFunction> mapper = Maps.newHashMap();
//        Map<String, Object> contextParamVals = Collections.emptyMap();
//        List<OutputParameter> outputParameters = jsonSplitterUDF.outParameters();

        replay();

        DataXCol2Index col2Index = DataXCol2Index.getCol2Index(
                Optional.of(transformerBuildInfo), runningContext, tab.getCols()); //new DataXCol2Index(mapper, contextParamVals, outputParameters);
        record.setCol2Index(col2Index);

        JSONObject extObj = new JSONObject();
        extObj.put(output_param_col1, 1);
        extObj.put(output_param_col2, 99);
        extObj.put(output_param_col3, "hello1");

        record.setString(key_col_ext, JsonUtil.toString(extObj, false));

        jsonSplitterUDF.evaluate(record);

        Object col1 = record.getColumn(prefix + output_param_col1);
        Assert.assertNotNull("col1 can not be null", col1);
        Assert.assertEquals((long) 1, col1);

        Object col2 = record.getColumn(prefix + output_param_col2);
        Assert.assertNotNull("col2 can not be null", col2);
        Assert.assertEquals((long) 99, col2);

        Object col3 = record.getColumn(prefix + output_param_col3);
        Assert.assertNotNull("col3 can not be null", col3);
        Assert.assertEquals("hello1", col3);


        /**
         * 第二轮测试 ext字段设置为空
         */
        record = new DefaultRecord();
        record.setCol2Index(col2Index);
        record.setString(key_col_ext, null);

        jsonSplitterUDF.evaluate(record);

        col1 = record.getColumn(prefix + output_param_col1);
        Assert.assertNull("col1 shall be null", col1);

        col2 = record.getColumn(prefix + output_param_col2);
        Assert.assertNull("col2 shall  be null", col2);

        col3 = record.getColumn(prefix + output_param_col3);
        Assert.assertNull("col3 shall  be null", col3);


        /**
         * 第三轮测试 ext字段设置为空字符串
         */
        record = new DefaultRecord();
        record.setCol2Index(col2Index);
        record.setString(key_col_ext, " ");

        jsonSplitterUDF.evaluate(record);

        col1 = record.getColumn(prefix + output_param_col1);
        Assert.assertNull("col1 shall be null", col1);

        col2 = record.getColumn(prefix + output_param_col2);
        Assert.assertNull("col2 shall  be null", col2);

        col3 = record.getColumn(prefix + output_param_col3);
        Assert.assertNull("col3 shall  be null", col3);

        /**
         * 第四轮测试 ext字段设置为非法的json内容
         */
        record = new DefaultRecord();
        record.setCol2Index(col2Index);
        record.setString(key_col_ext, "{aa:1");

        jsonSplitterUDF.evaluate(record);

        col1 = record.getColumn(prefix + output_param_col1);
        Assert.assertNull("col1 shall be null", col1);

        col2 = record.getColumn(prefix + output_param_col2);
        Assert.assertNull("col2 shall  be null", col2);

        col3 = record.getColumn(prefix + output_param_col3);
        Assert.assertNull("col3 shall  be null", col3);

        /**
         * 第五轮测试 ext字段设置为内容没有与outputParam相关的字段
         */
        record = new DefaultRecord();
        record.setCol2Index(col2Index);
        record.setString(key_col_ext, "{xxx:1," + output_param_col1 + ":66}");

        jsonSplitterUDF.evaluate(record);


        col1 = record.getColumn(prefix + output_param_col1);
        Assert.assertNotNull("col1 shall not be null", col1);
        Assert.assertEquals((long) 66, col1);

        col2 = record.getColumn(prefix + output_param_col2);
        Assert.assertNull("col2 shall  be null", col2);

        col3 = record.getColumn(prefix + output_param_col3);
        Assert.assertNull("col3 shall  be null", col3);


        // 第四轮和第三轮 测试 会发生 com.alibaba.fastjson2.JSONException 错误
        // 第五轮不应该有错误
        Assert.assertEquals(2, JSONSplitterUDF.jsonParseErrorCount);

        verifyAll();
    }

    /**
     * List<Cmeta> cols
     *
     * @return
     */
    private static List<CMeta> matchCols(SelectedTab tab) {
        EasyMock.reportMatcher(new ColsMatcher(tab));
        return null;
    }

    private static class ColsMatcher implements IArgumentMatcher {
        private final List<CMeta> expectCols;//  actualTab;

        public ColsMatcher(SelectedTab actualTab) {
            this.expectCols = actualTab.getCols();
        }

        @Override
        public boolean matches(Object argument) {
            List<CMeta> actualCols = (List<CMeta>) argument;
            return true;
        }

        @Override
        public void appendTo(StringBuffer buffer) {

        }
    }

    @Override
    protected OutParametersAndLiteriaAssert getOutParametersAndLiteriaAssert() {
        return new OutParametersAndLiteriaAssert() {
            @Override
            public void assertOutParameters(List<OutputParameter> outParameters) {
                Assert.assertTrue("outParameters"
                        , CollectionUtils.isEqualCollection(
                                Lists.newArrayList(prefix + output_param_col1, prefix + output_param_col2, prefix + output_param_col3)
                                , outParameters.stream().map((p) -> p.getName()).collect(Collectors.toList())));
            }

            @Override
            public void assertLiteriaDesc(List<UDFDesc> literiaDesc) {
                Assert.assertNotNull("literiaDesc can not be null", literiaDesc);
                Assert.assertEquals(2, literiaDesc.size());
            }
        };
    }
}
