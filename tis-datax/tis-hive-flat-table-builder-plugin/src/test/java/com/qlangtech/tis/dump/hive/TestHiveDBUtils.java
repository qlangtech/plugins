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

package com.qlangtech.tis.dump.hive;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author: baisui 百岁
 * @create: 2020-05-29 14:19
 **/
public class TestHiveDBUtils extends TestCase {

    public void testTetConnection() throws Exception {
        HiveDBUtils dbUtils = HiveDBUtils.getInstance("192.168.28.200:10000", "tis");

        Connection con = dbUtils.createConnection();

        // // Connection con = DriverManager.getConnection(
        // // "jdbc:hive://10.1.6.211:10000/tis", "", "");
        // System.out.println("start create connection");
        // // Connection con = DriverManager.getConnection(
        // // "jdbc:hive2://hadoop6:10001/tis", "", "");
        // System.out.println("create conn");
        System.out.println("start execute");
        Statement stmt = con.createStatement();
        //
        ResultSet result = stmt.executeQuery("select 1");
        System.out.println("wait receive");
        if (result.next()) {
            System.out.println(result.getInt(1));
        }
    }
}