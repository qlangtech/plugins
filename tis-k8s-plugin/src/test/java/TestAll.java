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

import com.qlangtech.tis.config.k8s.impl.TestDefaultK8SImage;
import com.qlangtech.tis.plugin.datax.TestK8SDataXJobWorker;
import com.qlangtech.tis.plugin.incr.TestDefaultIncrK8sConfig;
import com.qlangtech.tis.plugin.incr.TestK8sIncrSync;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/*
 * @create: 2020-01-14 09:15
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestAll extends TestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite();

        suite.addTestSuite(TestDefaultIncrK8sConfig.class);
        suite.addTestSuite(TestK8sIncrSync.class);
        suite.addTestSuite(TestK8SDataXJobWorker.class);
        suite.addTestSuite(TestDefaultK8SImage.class);


        return suite;
    }


}
