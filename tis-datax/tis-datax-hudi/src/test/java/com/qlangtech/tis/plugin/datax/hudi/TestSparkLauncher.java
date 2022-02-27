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

package com.qlangtech.tis.plugin.datax.hudi;

import org.junit.Test;

/**
 * https://archive.apache.org/dist/spark/spark-2.4.4/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-25 14:28
 **/
public class TestSparkLauncher {

    /**
     * ./spark-submit --jars /Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-utilities_2.11-0.10.0.jar,/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-spark-client-0.10.0.jar,/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/jcommander-1.72.jar,/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-sync-common-0.10.0.jar,/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-hive-sync-0.10.0.jar,/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-client-common-0.10.0.jar,/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-common-0.10.0.jar --master spark://192.168.28.201:7077  --class com.alibaba.datax.plugin.writer.hudi.TISHoodieDeltaStreamer \
     * /Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin.jar --table-type COPY_ON_WRITE   --source-class org.apache.hudi.utilities.sources.CsvDFSSource   --source-ordering-field last_ver    --target-base-path /user/hive/warehouse/customer_order_relation   --target-table customer_order_relation --props /user/admin/customer_order_relation-source.properties   --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * ./spark-submit --jars /Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-dependency/tis-datax-hudi-dependency/lib/hudi-utilities-bundle_2.11-0.10.0.jar  --master spark://192.168.28.201:7077  --class com.alibaba.datax.plugin.writer.hudi.TISHoodieDeltaStreamer       /Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-dependency/tis-datax-hudi-dependency/tis-datax-hudi-dependency-dist-3.5.0.jar --table-type COPY_ON_WRITE   --source-class org.apache.hudi.utilities.sources.CsvDFSSource   --source-ordering-field last_ver    --target-base-path hdfs://namenode/user/hive/warehouse/customer_order_relation   --target-table customer_order_relation/testDataX4465 --props hdfs://namenode/user/admin/customer_order_relation-source.properties   --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
     */


    @Test
    public void submitHudiJob() throws Exception {
        String[] str = new String[]{"1"};
        System.out.println(str[1]);
    }
}
