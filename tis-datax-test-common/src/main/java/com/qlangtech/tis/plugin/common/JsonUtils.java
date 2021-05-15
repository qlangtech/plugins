/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.qlangtech.tis.extension.impl.IOUtils;
import org.junit.Assert;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-14 09:15
 **/
public class JsonUtils {

    public static void assertJSONEqual(Class<?> invokeClass, String assertFileName, String actual) {
        String expectJson = com.alibaba.fastjson.JSON.toJSONString(
                JSON.parseObject(IOUtils.loadResourceFromClasspath(invokeClass, assertFileName))
                , SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.PrettyFormat);
        System.out.println(assertFileName + "\n" + expectJson);
        String actualJson = com.alibaba.fastjson.JSON.toJSONString(JSON.parseObject(actual)
                , SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.PrettyFormat);
        Assert.assertEquals("assertFile:" + assertFileName, expectJson, actualJson);
    }
}
