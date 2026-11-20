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

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.qlangtech.tis.common.utils.Assert;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.TargetColumn;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.DataType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-29 10:10
 **/
public class TestDataMaskingUDF extends BasicUDFDefinitionTest<DataMaskingUDF> {
    static final String addedField = "maskingUserName";
    static final String userName = "userName";

    @Override
    protected Class<DataMaskingUDF> getPluginClass() {
        return DataMaskingUDF.class;
    }

    @Override
    protected DataMaskingUDF createTransformerUDF() {
        return createDataMaskingUDF(1, 2);
    }

    private static DataMaskingUDF createDataMaskingUDF(int startIndex, int length) {
        DataMaskingUDF maskingUDF = new DataMaskingUDF();
        maskingUDF.from = userName;
        TargetColType toCol = new TargetColType();
        VirtualTargetColumn target = new VirtualTargetColumn();
        target.name = addedField;
        toCol.setTarget(target);
        toCol.setType(DataType.createVarChar(50));
        maskingUDF.to = toCol;// "maskingUserName";
        maskingUDF.start = startIndex;
        maskingUDF.length = length;
        maskingUDF.replaceChar = "*";
        return maskingUDF;
    }

    @Test
    @Override
    public void testEvaluate() {
        DataMaskingUDF transformerUDF = this.createTransformerUDF();
        DataMaskingUDF transformerUDFInfinit = this.createDataMaskingUDF(1,999999);
        Record record = Mockito.mock(Record.class, Mockito.withSettings().name("record"));

        Mockito.when(record.getString(userName)).thenReturn("baisuibaisui");

        transformerUDF.evaluate(record);
        transformerUDFInfinit.evaluate(record);

        Mockito.verify(record, Mockito.times(2)).getString(userName);
        Mockito.verify(record, Mockito.times(1)).setString(addedField, "b**suibaisui");
        Mockito.verify(record, Mockito.times(1)).setString(addedField, "b***********");
    }

    @Override
    protected OutParametersAndLiteriaAssert getOutParametersAndLiteriaAssert() {
        return new OutParametersAndLiteriaAssert() {
            @Override
            public void assertOutParameters(List<OutputParameter> outParameters) {
                Assert.assertTrue("outParameters"
                        , CollectionUtils.isEqualCollection(Collections.singletonList(addedField)
                                , outParameters.stream().map((p) -> p.getName()).collect(Collectors.toList())));
            }

            @Override
            public void assertLiteriaDesc(List<UDFDesc> literiaDesc) {
                Assert.assertNotNull("literiaDesc can not be null", literiaDesc);
                Assert.assertEquals(5, literiaDesc.size());
            }
        };
    }
}
