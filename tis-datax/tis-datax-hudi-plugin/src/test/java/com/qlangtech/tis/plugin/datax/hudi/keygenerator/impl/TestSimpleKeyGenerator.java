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

package com.qlangtech.tis.plugin.datax.hudi.keygenerator.impl;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.easymock.EasyMock;
import org.junit.Test;

import java.sql.Types;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-06-07 20:20
 **/
public class TestSimpleKeyGenerator implements TISEasyMock {

    @Test
    public void testDescJsonGen() {

        SuFormProperties.SuFormGetterContext context = new SuFormProperties.SuFormGetterContext();

        MockPlugin metaPlugin = this.mock("metaPlugin", MockPlugin.class);
        UploadPluginMeta param = this.mock("param", UploadPluginMeta.class);
        String id1 = "id1";
        EasyMock.expect(param.getExtraParam(
                IPropertyType.SubFormFilter.PLUGIN_META_SUBFORM_DETAIL_ID_VALUE)).andReturn(id1);

        List<ColumnMetaData> cols = Lists.newArrayList();
        // (int index, String key, DataType type, boolean pk)
        cols.add(new ColumnMetaData(0, "user_id", new com.qlangtech.tis.plugin.ds.DataType(Types.BIGINT), true));
        cols.add(new ColumnMetaData(1, "user_name", new com.qlangtech.tis.plugin.ds.DataType(Types.VARBINARY), false));
        EasyMock.expect(metaPlugin.getTableMetadata(id1)).andReturn(cols);
        context.plugin = metaPlugin;
        context.param = param;

        SuFormProperties.subFormGetterProcessThreadLocal.set(context);

        this.replay();
        PluginDesc.testDescGenerate(SimpleKeyGenerator.class, "simpleKeyGenerator_desc.json");

        this.verifyAll();
    }

    private interface MockPlugin extends Describable, DataSourceMeta {

    }
}