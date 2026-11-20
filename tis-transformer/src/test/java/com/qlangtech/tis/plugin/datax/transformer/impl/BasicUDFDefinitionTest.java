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

import com.google.common.collect.Sets;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.extension.util.impl.DefaultGroovyShellFactory;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.ThreadCacheTableCols;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.realtime.transfer.UnderlineUtils;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 09:59
 **/
public abstract class BasicUDFDefinitionTest<T extends UDFDefinition> implements TISEasyMock {
    public static final String addedField = "new_field_added";
    protected static final String userId = "user_id";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void startClearMocks() {
        this.clearMocks();
    }

    protected abstract Class<T> getPluginClass();

    protected abstract T createTransformerUDF();

    public abstract void testEvaluate();

    /**
     * List<SelectedTab> tabs
     *
     * @return
     */
    private static List<SelectedTab> matchtabs() {
        return Mockito.any();
    }

    @Test
    public void testDescJsonGen() throws Exception {
        DefaultGroovyShellFactory.setInConsoleModule();
        MockDataSourceMetaPlugin dsMetaPlugin = Mockito.mock(MockDataSourceMetaPlugin.class, Mockito.withSettings().name("dataSourceMetaPlugin"));


        IPluginStore<DataxReader> pluginStore = Mockito.mock(IPluginStore.class, Mockito.withSettings().name("pluginStore"));

        File tmpTabsDir = folder.newFolder("tmpTabs");


        Mockito.when(pluginStore.getTargetFileParentDir()).thenReturn(tmpTabsDir);

        List<SelectedTab> selectedTabs = TestSelectedTabs.createSelectedTabs(1);
        //  Mockito.when(dsMetaPlugin.fillSelectedTabMeta(matchtabs())).thenReturn(selectedTabs);

        List<ColumnMetaData> selectableCols = ColumnMetaData.convert(selectedTabs.get(0).getCols());
        ThreadCacheTableCols cacheTableCols = new ThreadCacheTableCols(dsMetaPlugin
                , selectedTabs.get(0).getEntityName(), (entity) -> {
            return selectedTabs.get(0).getCols();
        }, selectableCols);

        Mockito.when(dsMetaPlugin.getContextTableColsStream(Mockito.any(SuFormProperties.SuFormGetterContext.class))).thenReturn(cacheTableCols);

        UploadPluginMeta pluginMeta = UploadPluginMeta.parse("dataxReader:require");
        String testTable = TestSelectedTabs.tabNameOrderDetail;

        // List<ColumnMetaData> cols = TestSelectedTabs.tabColsMetaOrderDetail;


        //  Mockito.when(dsMetaPlugin.getTableMetadata(false, null, EntityName.parse(testTable))).thenReturn(cols);

        SuFormProperties.setSuFormGetterContext(dsMetaPlugin, pluginStore, pluginMeta, testTable);

        SelectedTab.getTmpTableStoreFile(pluginStore, testTable)
                .write(selectedTabs.get(0), Sets.newHashSet());

        Class<T> pluginClass = getPluginClass();
        PluginDesc.testDescGenerate(pluginClass
                , UnderlineUtils.addUnderline(pluginClass.getSimpleName())
                        + "/descriptor.json");

        Mockito.verify(dsMetaPlugin).getContextTableColsStream(Mockito.any(SuFormProperties.SuFormGetterContext.class));
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
