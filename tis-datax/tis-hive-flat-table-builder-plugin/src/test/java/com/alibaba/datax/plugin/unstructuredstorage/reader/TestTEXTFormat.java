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

package com.alibaba.datax.plugin.unstructuredstorage.reader;

import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import junit.framework.TestCase;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestTEXTFormat extends TestCase {
    public void testRowIteratorRead() {
        // BufferedReader reader, boolean skipHeader, int colSize, Character fieldDelimiter

        IOUtils.loadResourceFromClasspath(TestTEXTFormat.class, "./instancedetail_hdfs_format_text.txt", true, (input) -> {

            BufferedReader reader = new BufferedReader(new InputStreamReader(input, TisUTF8.get()));
            boolean skipHeader = true;
            int colSize = 59;
            Character fieldDelimiter = Delimiter.Char001.val;
            TEXTFormat txtFormat = new TEXTFormat(reader, skipHeader, colSize, fieldDelimiter);

            String[] row = null;
            int rowCount = 0;
            while (txtFormat.hasNext()) {
                row = txtFormat.next();
                Assert.assertEquals(colSize, row.length);
                rowCount++;
                if (rowCount == 1) {
                    Assert.assertEquals("fff", row[58]);
                }
            }

            Assert.assertEquals("rowCount must be 20", 20, rowCount);
            return null;
        });

    }
}
