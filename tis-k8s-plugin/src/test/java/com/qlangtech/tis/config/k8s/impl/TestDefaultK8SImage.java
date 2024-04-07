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

package com.qlangtech.tis.config.k8s.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage.DescriptorImpl;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.junit.Assert;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-15 15:36
 **/
public class TestDefaultK8SImage extends TestCase {

    public void testHostAliasesValidate() {
        //https://blog.csdn.net/weixin_47729423/article/details/114288431
        DefaultK8SImage k8SImage = new DefaultK8SImage();
        DefaultK8SImage.DescriptorImpl descriptor = (DefaultK8SImage.DescriptorImpl) k8SImage.getDescriptor();
        assertNotNull("descriptor can not be null", descriptor);

        IFieldErrorHandler msgHandler = EasyMock.createMock("msgHandler", IFieldErrorHandler.class);
        Context context = EasyMock.createMock("context", Context.class);
        String fieldName = "hostAliases";

        EasyMock.replay(msgHandler, context);
        assertTrue(descriptor.validateHostAliases(msgHandler, context, fieldName
                , IOUtils.loadResourceFromClasspath(DefaultK8SImage.class, "DefaultK8SImage-hostaliases-content.yaml")));

        EasyMock.verify(msgHandler, context);
    }

    public void testValidateImagePath() {
        //  DefaultK8SImage k8SImage = new DefaultK8SImage();
        DefaultK8SImage.DescriptorImpl descriptor = new DefaultK8SImage.DescriptorImpl();
        IFieldErrorHandler msgHandler = EasyMock.createMock("msgHandler", IFieldErrorHandler.class);
        Context context = EasyMock.createMock("context", Context.class);
        String fieldName = "imagePath";

        // "([^/]+\\.[^/.]+/)? ([^/.]+/)?[^/.]+(:.+)?"



        msgHandler.addFieldError(context, fieldName, "不符合格式:" + DescriptorImpl.PATTERN_IMAGE_PATH);
        EasyMock.expectLastCall().times(2);
        EasyMock.replay(msgHandler, context);

        String flinkImagePath = "registry.cn-hangzhou.aliyuncs.com/tis/flink:4.0.1.2";
        Assert.assertTrue(descriptor.validateImagePath(msgHandler, context, fieldName, flinkImagePath));

        flinkImagePath = "tis/flink:4.0.1.2";
        Assert.assertTrue(descriptor.validateImagePath(msgHandler, context, fieldName, flinkImagePath));

        flinkImagePath = "flink:4.0.1.2";
        Assert.assertTrue(descriptor.validateImagePath(msgHandler, context, fieldName, flinkImagePath));

        flinkImagePath = "flink:latest";
        Assert.assertTrue(descriptor.validateImagePath(msgHandler, context, fieldName, flinkImagePath));

        flinkImagePath = "fl ink:latest";
        Assert.assertFalse(descriptor.validateImagePath(msgHandler, context, fieldName, flinkImagePath));

        flinkImagePath = "flink:\tlatest";
        Assert.assertFalse(descriptor.validateImagePath(msgHandler, context, fieldName, flinkImagePath));

        EasyMock.verify(msgHandler, context);
    }


    public void testPluginDescGenerate() {

        PluginDesc.testDescGenerate(DefaultK8SImage.class, "DefaultK8SImage-desc.json");
    }
}
