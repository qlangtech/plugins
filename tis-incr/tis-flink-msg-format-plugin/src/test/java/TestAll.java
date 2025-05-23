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

import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactoryTest;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.TestTISCanalJsonFormatFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.TestTISSinkDebeziumJsonFormatFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.canaljson.TISCanalJsonFormatFactoryTest;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json.SourceJsonFormatFactoryTest;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json.TestSinkJsonFormatFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json.TestSourceJsonFormatFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-24 18:25
 **/

@RunWith(Suite.class)
@Suite.SuiteClasses(
        {
                TestTISCanalJsonFormatFactory.class,
                TestTISSinkDebeziumJsonFormatFactory.class,
                TestSinkJsonFormatFactory.class,
                TestSourceJsonFormatFactory.class,
                TISCanalJsonFormatFactoryTest.class,
                SourceJsonFormatFactoryTest.class,
                FormatFactoryTest.class
        })
public class TestAll {
}
