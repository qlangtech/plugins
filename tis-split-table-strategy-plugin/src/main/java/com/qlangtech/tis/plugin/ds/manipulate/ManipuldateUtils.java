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
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.ds.DBIdentity;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.IPluginItemsProcessor;
import com.qlangtech.tis.util.IUploadPluginMeta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-10 23:16
 **/
public class ManipuldateUtils {
    public static IPluginItemsProcessor cloneInstance(IPluginContext pluginContext, Context context, String newIdentityName
            , Consumer<IUploadPluginMeta> pluginMetaConsumer
            , Consumer<String> originIdentityIdConsumer) {
        Objects.requireNonNull(context, "param content can not be null");
        JSONObject postContent = Objects.requireNonNull(pluginContext, "pluginContext can not be null").getJSONPostContent();
        JSONObject manipulateTarget = postContent.getJSONObject("manipulateTarget");
        final String keyManipulatePluginMeta = "manipulatePluginMeta";
        String pluginType = postContent.getString(keyManipulatePluginMeta);
        if (StringUtils.isEmpty(pluginType)) {
            throw new IllegalArgumentException("post payload " + keyManipulatePluginMeta + " relevant value can not be null");
        }
        /**
         * 将目标插件的ID修改，进行保存
         */
        List<IUploadPluginMeta> pluginMeta = pluginContext.parsePluginMeta(new String[]{pluginType}, false);
        if (CollectionUtils.isEmpty(pluginMeta)) {
            throw new IllegalStateException("pluginMeta can not be empty");
        }
        for (IUploadPluginMeta meta : pluginMeta) {
            pluginMetaConsumer.accept(meta);

            JSONArray itemsArray = new JSONArray();
            itemsArray.add(manipulateTarget);
            Pair<Boolean, IPluginItemsProcessor> pluginItems
                    = pluginContext.getPluginItems(meta, context, 0, itemsArray, false, ((propType, val) -> {
                PropertyType ptype = (PropertyType) propType;
                if (ptype.isIdentity()) {
                    originIdentityIdConsumer.accept((String) val);
                }
                // 将原先的主键覆盖掉
                return ptype.isIdentity() ? newIdentityName : val;
            }));

            if (context.hasErrors()) {
                return null;
            }

            if (pluginItems.getKey()) {
                throw new IllegalStateException("pluginItems parse faild");
            }
            IPluginItemsProcessor itemsProcessor = pluginItems.getRight();
            //
            return itemsProcessor;
        }

        throw new IllegalStateException("can not reach here");
    }
}
