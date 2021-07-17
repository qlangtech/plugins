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

package com.qlangtech.tis.config.k8s.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import junit.framework.TestCase;
import org.easymock.EasyMock;

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


    public void testPluginDescGenerate() {

        PluginDesc.testDescGenerate(DefaultK8SImage.class, "DefaultK8SImage-desc.json");
    }
}
