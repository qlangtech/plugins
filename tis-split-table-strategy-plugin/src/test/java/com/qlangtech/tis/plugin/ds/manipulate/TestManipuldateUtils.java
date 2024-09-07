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

package com.qlangtech.tis.plugin.ds.manipulate;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.runtime.module.misc.IPostContent;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.IPluginItemsProcessor;
import com.qlangtech.tis.util.IUploadPluginMeta;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang3.tuple.Pair;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-20 08:28
 **/
public class TestManipuldateUtils implements TISEasyMock {

    @Test
    public void testCloneInstance() {

        IPluginContext pluginContext = mock("pluginContext", IPluginContext.class);
        IPluginItemsProcessor itemProcessor = mock("itemProcessor", IPluginItemsProcessor.class);
        JSONObject postJson = JsonUtil.loadJSON(ManipuldateUtils.class, "post-manipulate-body.json");
        EasyMock.expect(pluginContext.getJSONPostContent()).andReturn(postJson);

        String meta = "appSource:require,update_true,justGetItemRelevant_true,dataxName_mysql_mysql,processModel_createDatax";
        List<IUploadPluginMeta> pluginMetas = (UploadPluginMeta.parse(pluginContext, new String[]{meta}, false));
        EasyMock.expect(pluginContext.parsePluginMeta(new String[]{meta}, false))
                .andReturn(pluginMetas);


        Context context = mock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(true);

        for (IUploadPluginMeta m : pluginMetas) {

            JSONArray itemsArray = new JSONArray();
            itemsArray.add(postJson.getJSONObject(IUploadPluginMeta.KEY_JSON_MANIPULATE_TARGET));

            pluginContext.getPluginItems(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.eq(0), EasyMock.anyObject(), EasyMock.eq(false), EasyMock.anyObject());
            IExpectationSetters<Pair<Boolean, IPluginItemsProcessor>> getPluginItemsSetters = EasyMock.expectLastCall();
            getPluginItemsSetters.andStubDelegateTo(new DelegatePostContent(itemProcessor));

        }

        String newIdentityName = "test";
        Consumer<IUploadPluginMeta> pluginMetaConsumer = (mt) -> {
        };


        replay();
        ManipulateItemsProcessor itemsProcessor
                = ManipuldateUtils.instance(pluginContext, context, newIdentityName, pluginMetaConsumer);
        Assert.assertNull("because newIdentityName is duplicate, result itemsProcessor shall be null", itemsProcessor);

        verifyAll();
    }

    private class DelegatePostContent implements IPostContent {
        private final IPluginItemsProcessor itemsProcessor;

        public DelegatePostContent(IPluginItemsProcessor itemsProcessor) {
            this.itemsProcessor = itemsProcessor;
        }

        @Override
        public Pair<Boolean, IPluginItemsProcessor> getPluginItems(
                IUploadPluginMeta pluginMeta, Context context, int pluginIndex, JSONArray itemsArray, boolean verify, PropValRewrite propValRewrite) {
            UploadPluginMeta meta = (UploadPluginMeta) pluginMeta;
            // 要保证是insert 操作
            Assert.assertFalse("shall not be update", meta.isUpdate());
            return Pair.of(false, itemsProcessor);
        }

        @Override
        public List<IUploadPluginMeta> parsePluginMeta(String[] plugins, boolean useCache) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JSONObject getJSONPostContent() {
            throw new UnsupportedOperationException();
        }
    }
}
