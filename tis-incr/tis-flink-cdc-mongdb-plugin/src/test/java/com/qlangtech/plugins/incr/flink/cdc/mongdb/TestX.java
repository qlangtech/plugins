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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import org.junit.Test;

import java.net.URL;
import java.util.Enumeration;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-26 10:05
 **/
public class TestX {

    @Test
    public void test() throws Exception {
        Enumeration<URL> resources = TestX.class.getClassLoader().getResources("io/debezium/embedded/EmbeddedEngineChangeEvent.class");
        while (resources.hasMoreElements()) {
            System.out.println(resources.nextElement());
        }
    }
}
