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

package com.qlangtech.plugins.incr.flink.metrics;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

/**
 * TIS通过 grpc 协议将flink metric 信息反馈给TIS节点汇总
 * https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/config/#metrics-reporter-%3Cname%3E-factory-class
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-14 10:22
 **/
public class TISPBReporterFactory implements MetricReporterFactory {
    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new TISPBReporter();
    }
}
