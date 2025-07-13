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

import com.alibaba.datax.common.element.Record;
import com.qlangtech.tis.common.utils.Assert;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import org.apache.commons.collections.CollectionUtils;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-13 16:18
 **/
public class TestRegexPatternReplace extends BasicUDFDefinitionTest<RegexPatternReplace> {

    @Override
    protected Class<RegexPatternReplace> getPluginClass() {
        return RegexPatternReplace.class;
    }

    @Override
    protected RegexPatternReplace createTransformerUDF() {
        RegexPatternReplace replace = new RegexPatternReplace();
        replace.pattern = "^(\\d{4}-\\d{2})-\\d{2}$";
        replace.replaceContent = "$1";
        replace.from = userId;
        replace.throwErrorNotMatch = true;
        return replace;
    }

    @Test
    @Override
    public void testEvaluate() {
        RegexPatternReplace replaceUDF = this.createTransformerUDF();

        Record record = mock("record", Record.class);
        //Column usrIdCol = mock("userIdCol", Column.class);
        EasyMock.expect(record.getString(userId)).andReturn("2018-08-26");
        record.setString(userId, "2018-08");
        EasyMock.expectLastCall().times(1);
        this.replay();
        replaceUDF.evaluate(record);

        this.verifyAll();
    }

    @Override
    protected OutParametersAndLiteriaAssert getOutParametersAndLiteriaAssert() {

        return new OutParametersAndLiteriaAssert() {
            @Override
            public void assertOutParameters(List<OutputParameter> outParameters) {
                Assert.assertTrue("outParameters"
                        , CollectionUtils.isEqualCollection(Collections.singletonList(userId)
                                , outParameters.stream().map((p) -> p.getName()).collect(Collectors.toList())));
            }

            @Override
            public void assertLiteriaDesc(List<UDFDesc> literiaDesc) {
                Assert.assertNotNull("literiaDesc can not be null", literiaDesc);
                Assert.assertEquals(1, literiaDesc.size());
            }
        };

    }
}