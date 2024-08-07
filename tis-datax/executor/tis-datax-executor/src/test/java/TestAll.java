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

import com.google.common.collect.Lists;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-04 10:19
 **/
public class TestAll extends TestCase {

    public void testA() {
//        List<String> list = Lists.newArrayListWithCapacity(2);
//        list.set(0, "hello");
        String[] array = new String[2];
        array[1] = "hello";
        ArrayList<String> strings = Lists.newArrayList(array);
        for (String v : strings) {
            System.out.println(v);
        }
    }

    public static Test suite() throws Exception {
        TestSuite suite = new TestSuite();
        // suite.addTestSuite(TestDataxExecutor.class);
        // suite.addTestSuite(TestDataxExecutorSynRes.class);
        return suite;
    }
}
