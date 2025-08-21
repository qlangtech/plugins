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

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-08-21 12:12
 **/
public class BatchUpdate {

    @Test()
    public void testBatchUpdate() {
        DataSourceFactory dataSource = TIS.getDataBasePlugin(PostedDSProp.parse("order"));

        dataSource.visitFirstConnection((conn) -> {
            int count = 0;
            while (true) {
                try {
                    conn.execute("update orderdetail_02 set op_time=op_time+1 where last_ver = " + ((count++) % 13));
                    Thread.sleep(1000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }

        });
    }
}
