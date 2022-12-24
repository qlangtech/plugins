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

package com.qlangtech.tis.plugin.ds.split;

import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.plugin.ds.TableInDB;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-12-24 15:31
 **/
public class TestDefaultSplitTableStrategy {

    @Test
    public void testTabAggre() {
        // final String jdbcUrl = "jdbc_url_1";
        final String dataXCfgFileName = "base_0.json";
        DefaultSplitTableStrategy splitTableStrategy = new DefaultSplitTableStrategy();
        TableInDB tableInDB = splitTableStrategy.createTableInDB();
        String[] splitTabs = new String[]{"base_01", "base", "base_02", "order"};
        for (String tab : splitTabs) {
            tableInDB.add(DataXJobSubmit.TableDataXEntity.TEST_JDBC_URL, tab);
        }

        DataXJobInfo baseJobInfo
                = tableInDB.createDataXJobInfo(DataXJobSubmit.TableDataXEntity.createTableEntity4Test(dataXCfgFileName, "base"));
        Optional<String[]> targetTableNames = baseJobInfo.getTargetTableNames();

        Assert.assertTrue(targetTableNames.isPresent());
        String[] baseTabs = targetTableNames.get();
        Assert.assertEquals(String.join(",", baseTabs), 3, baseTabs.length);
    }
}
