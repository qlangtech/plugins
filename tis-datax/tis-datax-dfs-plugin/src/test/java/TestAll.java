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

import com.qlangtech.tis.plugin.datax.TestDataXDFSReader;
import com.qlangtech.tis.plugin.datax.TestDataXDFSReaderWithMeta;
import com.qlangtech.tis.plugin.datax.TestDataXDFSWriter;
import com.qlangtech.tis.plugin.datax.TestDataXDFSWriterReal;
import com.qlangtech.tis.plugin.datax.format.TestCSVFormat;
import com.qlangtech.tis.plugin.datax.format.TestTextFormat;
import com.qlangtech.tis.plugin.datax.resmatcher.TestMetaAwareDFSResMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-09 09:22
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses(
        {TestDataXDFSReader.class
                , TestDataXDFSWriter.class
                , TestDataXDFSWriterReal.class
                , TestDataXDFSReaderWithMeta.class
                , TestCSVFormat.class
                , TestTextFormat.class
                , TestMetaAwareDFSResMatcher.class})
public class TestAll {
//    public static Test suite() {
//        TestSuite suite = new TestSuite();
//        suite.addTestSuite(TestDataXFtpReader.class);
//        suite.addTestSuite(TestDataXFtpWriter.class);
//        return suite;
//    }
}