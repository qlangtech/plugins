/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.hive;

import com.qlangtech.tis.manage.common.TisUTF8;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2020-06-03 13:57
 **/
public class TestHiveInsertFromSelectParser extends TestCase {

    public void testSqlParse() throws Exception {
        HiveInsertFromSelectParser parse = new HiveInsertFromSelectParser();

        try (InputStream input = TestHiveInsertFromSelectParser.class.getResourceAsStream("tmp_pay.sql")) {
            parse.start(IOUtils.toString(input, TisUTF8.get()));
            System.out.println("getTargetTableName:" + parse.getTargetTableName());
            List<HiveColumn> columns = parse.getColsExcludePartitionCols();
            assertEquals(5, columns.size());
            assertEquals("totalpay_id totalpay_id", columns.get(0).toString());
            assertEquals("kindpay kindpay", columns.get(1).toString());
            assertEquals("fee fee", columns.get(2).toString());
            assertEquals("is_enterprise_card_pay is_enterprise_card_pay", columns.get(3).toString());
            assertEquals("pay_customer_ids pay_customer_ids", columns.get(4).toString());

            for (HiveColumn c : columns) {
                System.out.println(c.getName());
            }
        }


    }
}
