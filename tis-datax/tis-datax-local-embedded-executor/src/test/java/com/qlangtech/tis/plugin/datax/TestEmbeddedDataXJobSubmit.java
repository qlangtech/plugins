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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.PreviewRowsData;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-22 09:48
 **/
public class TestEmbeddedDataXJobSubmit extends TestCase {

    public void testPreviewRowsData() {
        EmbeddedDataXJobSubmit embeddedDataXJobSubmit = new EmbeddedDataXJobSubmit();
        int pageSize = 10;
        PreviewRowsData records = embeddedDataXJobSubmit.previewRowsData("mysql_mysql", "base", "testid", true, true, 10);
        Assert.assertTrue(records.getRows().size() > 0);
    }
}
