/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import com.qlangtech.tis.plugin.datax.TestDataXGlobalConfig;
import com.qlangtech.tis.plugin.datax.TestDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.TestDefaultDataxProcessor;
import com.qlangtech.tis.plugin.datax.common.TestBasicDataXRdbmsWriter;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author: baisui 百岁
 * @create: 2021-04-21 11:31
 **/
public class TestAll extends TestCase {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestDataXGlobalConfig.class);
        suite.addTestSuite(TestDataXJobSubmit.class);
        suite.addTestSuite(TestDefaultDataxProcessor.class);
        suite.addTestSuite(TestBasicDataXRdbmsWriter.class);
        return suite;
    }



}
