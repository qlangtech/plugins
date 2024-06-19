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

import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.impl.ConcatUDF.Separator;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;

import java.util.List;

/**
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 10:59
 **/
public class TestConcatUDF extends BasicUDFDefinitionTest<ConcatUDF> {
    @Override
    protected Class<ConcatUDF> getPluginClass() {
        return ConcatUDF.class;
    }

    @Override
    protected ConcatUDF createTransformerUDF() {
        ConcatUDF concatUDF = new ConcatUDF();
        List<TargetColType> fromCols = Lists.newArrayList();
        TargetColType to = new TargetColType();
        concatUDF.from = fromCols;
        concatUDF.to = to;
        concatUDF.separator = Separator.Cut.name();
        return concatUDF;
    }

    @Override
    public void testEvaluate() {

    }

    @Override
    protected OutParametersAndLiteriaAssert getOutParametersAndLiteriaAssert() {
        return new OutParametersAndLiteriaAssert(){
            @Override
            public void assertOutParameters(List<String> outParameters) {

            }

            @Override
            public void assertLiteriaDesc(List<UDFDesc> literiaDesc) {

            }
        };
    }
}
