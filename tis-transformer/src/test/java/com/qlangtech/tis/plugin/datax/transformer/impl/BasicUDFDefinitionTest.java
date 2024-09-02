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

import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.extension.util.impl.DefaultGroovyShellFactory;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.realtime.transfer.UnderlineUtils;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 09:59
 **/
public abstract class BasicUDFDefinitionTest<T extends UDFDefinition> implements TISEasyMock {
    @Before
    public void startClearMocks() {
        this.clearMocks();
    }

    protected abstract Class<T> getPluginClass();

    protected abstract T createTransformerUDF();

    public abstract void testEvaluate();

    @Test
    public void testDescJsonGen() throws Exception {
        DefaultGroovyShellFactory.setInConsoleModule();
        DataSourceMetaPlugin dsMetaPlugin = mock("dataSourceMetaPlugin", DataSourceMetaPlugin.class);
        UploadPluginMeta pluginMeta = UploadPluginMeta.parse("dataxReader:require");
        String testTable = TestSelectedTabs.tabNameOrderDetail;

        List<ColumnMetaData> cols = TestSelectedTabs.tabColsMetaOrderDetail;


        EasyMock.expect(dsMetaPlugin.getTableMetadata(false, EntityName.parse(testTable))).andReturn(cols);

        SuFormProperties.setSuFormGetterContext(dsMetaPlugin, pluginMeta, testTable);


        this.replay();
        Class<T> pluginClass = getPluginClass();
        PluginDesc.testDescGenerate(pluginClass
                , UnderlineUtils.addUnderline(pluginClass.getSimpleName())
                        + "/descriptor.json");

        this.verifyAll();
    }

    @Test
    public void testOutParametersAndLiteria() {
        T cpValueUDF = this.createTransformerUDF();

        OutParametersAndLiteriaAssert makeAssert = this.getOutParametersAndLiteriaAssert();



        List<OutputParameter> outParameters = cpValueUDF.outParameters();

        makeAssert.assertOutParameters(outParameters);

//        Assert.assertTrue("outParameters"
//                , CollectionUtils.isEqualCollection(Collections.singletonList(addedField), outParameters));

        List<UDFDesc> literiaDesc = cpValueUDF.getLiteria();

        makeAssert.assertLiteriaDesc(literiaDesc);

//        Assert.assertNotNull("literiaDesc can not be null", literiaDesc);
//        Assert.assertEquals(2, literiaDesc.size());

    }

    protected abstract OutParametersAndLiteriaAssert getOutParametersAndLiteriaAssert();


    interface OutParametersAndLiteriaAssert {

        void assertOutParameters(List<OutputParameter> outParameters);

        void assertLiteriaDesc(List<UDFDesc> literiaDesc);
    }

}
