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

package com.qlangtech.tis.plugin.datax.common;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-16 13:17
 **/
public class TestBasicDataXRdbmsWriter extends TestCase {


    public void testParseStatements() {


        String createTab = "CREATE TABLE \"orderdetail\"\n" +
                "(\n" +
                "    \"order_id\"             VARCHAR2(60 CHAR),\n" +
                "    \"global_code\"          VARCHAR2(26 CHAR)\n" +
                " , CONSTRAINT orderdetail_pk PRIMARY KEY (\"order_id\")\n" +
                ")";
        String createComment = "COMMENT ON COLUMN \"orderdetail\".\"global_code\" IS '1,正常开单;2预订开单;3.排队开单;4.外卖开单'";


        List<String> statements = BasicDataXRdbmsWriter.parseStatements(
                createTab + ";\n" +
                        createComment);

        Assert.assertEquals(2, statements.size());

        Assert.assertEquals(createTab, statements.get(0));
        Assert.assertEquals(createComment, statements.get(1));


        statements = BasicDataXRdbmsWriter.parseStatements(
                createTab + "; " +
                        createComment + ";\n");
        Assert.assertEquals(2, statements.size());

        Assert.assertEquals(createTab, statements.get(0));
        Assert.assertEquals(createComment, statements.get(1));
    }
}
