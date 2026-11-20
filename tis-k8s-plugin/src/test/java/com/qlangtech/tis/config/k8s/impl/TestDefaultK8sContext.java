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

import com.qlangtech.tis.plugin.common.PluginDesc;
import org.junit.Test;
/*
 * @create: 2020-02-10 14:19
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestDefaultK8sContext {

//    public void testValidate() {
//    }


    @Test
    public void testPluginDescGenerate() {
        PluginDesc.testDescGenerate(DefaultK8sContext.class, "DefaultK8sContext-desc.json");
    }

    public void test() {
        int num = 2;
        switch (num) {
            case 1:
                System.out.println(1);
            case 2:
                System.out.println(2);
            case 3:
                System.out.println(3);
            default:
        }
    }
}
