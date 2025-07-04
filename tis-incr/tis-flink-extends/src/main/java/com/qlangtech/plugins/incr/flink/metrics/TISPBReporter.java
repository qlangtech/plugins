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

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.Map;

/**
 * https://github.com/datavane/tis/issues/397
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-14 10:24
 **/
public class TISPBReporter extends AbstractReporter implements Scheduled {
    @Override
    public String filterCharacters(String s) {
        return CharacterFilter.NO_OP_FILTER.filterCharacters(s);
    }

    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void report() {

        for (Map.Entry<Counter, String> entry : counters.entrySet()) {
            Counter counter = entry.getKey();
            String metricName = entry.getValue();
            System.out.println(metricName + ": " + counter.getCount());
        }

    }
}
