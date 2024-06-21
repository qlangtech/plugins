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
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.DataType;
import org.apache.commons.collections.CollectionUtils;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-10 11:38
 **/
public class TestCopyValUDF extends BasicUDFDefinitionTest<CopyValUDF> {


    static final String addedField = "new_field_added";
    static final String userId = "user_id";

    @Override
    protected OutParametersAndLiteriaAssert getOutParametersAndLiteriaAssert() {

        return new OutParametersAndLiteriaAssert() {
            @Override
            public void assertOutParameters(List<TargetColType> outParameters) {
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

    @Override
    protected CopyValUDF createTransformerUDF() {
        CopyValUDF cpValueUDF = new CopyValUDF();
        TargetColType typeCol = new TargetColType();
        VirtualTargetColumn virtualCol = new VirtualTargetColumn();
        virtualCol.name = addedField;
        typeCol.setTarget(virtualCol);
        typeCol.setType(DataType.createVarChar(32));
        cpValueUDF.to = typeCol;
        cpValueUDF.from = userId;
        return cpValueUDF;
    }

    @Test
    @Override
    public void testEvaluate() {
        CopyValUDF cpValueUDF = this.createTransformerUDF();

        Record record = mock("record", Record.class);
        Column usrIdCol = mock("userIdCol", Column.class);
        EasyMock.expect(record.getColumn(userId)).andReturn(usrIdCol);
        record.setColumn(addedField, usrIdCol);
        EasyMock.expectLastCall().times(1);
        this.replay();
        cpValueUDF.evaluate(record);

        this.verifyAll();
    }

    @Override
    protected Class<CopyValUDF> getPluginClass() {
        return CopyValUDF.class;
    }
}
