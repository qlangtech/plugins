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
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.impl.AutoGenerateBigIntIDUDF.GenerateValueType;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.DataType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.transformer.impl.TestCopyValUDF.userId;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-04-03 22:37
 **/
public class TestAutoGenerateBigIntIDUDF extends BasicUDFDefinitionTest<AutoGenerateBigIntIDUDF> {
    @Override
    protected Class<AutoGenerateBigIntIDUDF> getPluginClass() {
        return AutoGenerateBigIntIDUDF.class;
    }

    @Override
    protected AutoGenerateBigIntIDUDF createTransformerUDF() {
        AutoGenerateBigIntIDUDF autoGen = new AutoGenerateBigIntIDUDF();
        TargetColType typeCol = new TargetColType();
        VirtualTargetColumn virtualCol = new VirtualTargetColumn();
        virtualCol.name = addedField;
        typeCol.setTarget(virtualCol);
        typeCol.setType(DataType.createVarChar(32));
        autoGen.valType = GenerateValueType.AUTO_SNOWFLAKE_ID.token;
        autoGen.to = typeCol;
        return autoGen;
    }


    @Test
    @Override
    public void testEvaluate() {
        AutoGenerateBigIntIDUDF autoGenUDF = this.createTransformerUDF();

        Record record = Mockito.mock(Record.class, Mockito.withSettings().name("record"));
        // Column usrIdCol = Mockito.mock(Column.class, Mockito.withSettings().name("userIdCol"));
        // Mockito.when(record.getColumn(userId)).thenReturn(usrIdCol);
        autoGenUDF.evaluate(record);

        Mockito.verify(record, Mockito.times(1)).setColumn(Mockito.eq(addedField), Mockito.anyLong());
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
                Assert.assertEquals(2, literiaDesc.size());
            }
        };
    }
}
