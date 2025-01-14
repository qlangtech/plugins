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

package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestDefaultHiveConnGetter {
    static final String replacePt = "2023110305";

    @Test
    public void testReplaceLatestPattern() {
        Assert.assertEquals("pt = '2023110305' and pmod='1'"
                , replaceLastestPtCriteria(IDumpTable.PARTITION_PT + " = " + HiveTable.KEY_PT_LATEST + " and pmod='1'"));

        Assert.assertEquals("pt > '2023110305' and pmod='1'"
                , replaceLastestPtCriteria(IDumpTable.PARTITION_PT + " > " + HiveTable.KEY_PT_LATEST + " and pmod='1'"));

        Assert.assertEquals("pt >= '2023110305' "
                , replaceLastestPtCriteria(IDumpTable.PARTITION_PT + " >= " + HiveTable.KEY_PT_LATEST + " "));

        Assert.assertEquals("pt >= 2023110305 "
                , replaceLastestPtCriteria(IDumpTable.PARTITION_PT + " >= " + HiveTable.KEY_PT_LATEST + " ", false));

        Assert.assertEquals("pt = 2023110305 and pmod='1'"
                , replaceLastestPtCriteria(IDumpTable.PARTITION_PT + " = " + HiveTable.KEY_PT_LATEST + " and pmod='1'", false));
    }

    private static String replaceLastestPtCriteria(String latestFilter) {
        return replaceLastestPtCriteria(latestFilter, true);
    }

    private static String replaceLastestPtCriteria(String latestFilter, boolean isStrType) {
        String s = DefaultHiveConnGetter.replaceLastestPtCriteria(latestFilter, (ptKey) -> {
            Assert.assertEquals(IDumpTable.PARTITION_PT, ptKey);
            return Pair.of(isStrType, replacePt);
        });
        return s;
    }
}
