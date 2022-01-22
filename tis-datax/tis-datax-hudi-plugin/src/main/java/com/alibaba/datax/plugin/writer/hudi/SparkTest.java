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

package com.alibaba.datax.plugin.writer.hudi;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-21 18:43
 **/
public class SparkTest {
    public static void main(String[] args) {
        // Spark session setup..
        SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.ui.enabled", "false")
                .master("local[1]").getOrCreate();
        JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
        spark.sparkContext().setLogLevel("WARN");
    }
}
