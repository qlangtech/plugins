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

package com.qlangtech.tis.dump.hive;

import com.google.common.collect.Sets;
import com.qlangtech.tis.fs.FSHistoryFileUtils;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.plugin.datax.MREngine;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import org.junit.Test;
/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-29 15:07
 **/
public class TestHiveRemoveHistoryDataTask {

    @Test
    public void testDropHistoryHiveTable() throws Exception {
        String dbName = "testdb";
        String tabName = "order";


        DataSourceMeta sourceMeta = Mockito.mock(DataSourceMeta.class, Mockito.withSettings().name("sourceMeta"));

        ITISFileSystem fileSystem = Mockito.mock(ITISFileSystem.class, Mockito.withSettings().name("fileSystem"));
        JDBCConnection hiveConn = Mockito.mock(JDBCConnection.class, Mockito.withSettings().name("hiveConn"));

        Statement showDBStatment = Mockito.mock(Statement.class, Mockito.withSettings().name("showDBStatment"));
        ResultSet resultSet = Mockito.mock(ResultSet.class, Mockito.withSettings().name("resultSet"));


        Mockito.when(showDBStatment.executeQuery("show databases")).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(resultSet.getString(1)).thenReturn(dbName);

        showDBStatment.close();
        resultSet.close();


        // 查询表
        Statement showTabsStatement = Mockito.mock(Statement.class, Mockito.withSettings().name("showTabsStatement"));
        ResultSet tabsResult = Mockito.mock(ResultSet.class, Mockito.withSettings().name("tabsResult"));
        Mockito.when(tabsResult.next()).thenReturn(true).thenReturn(false);
        Mockito.when(tabsResult.getString(1)).thenReturn(tabName);
        tabsResult.close();
        Mockito.when(showTabsStatement.executeQuery("show tables in testdb")).thenReturn(tabsResult);
        showTabsStatement.close();

        String minPt1 = "20210529142016";
        String minPt2 = "20210529152016";

        String retainPt1 = "20210529162016";
        String retainPt2 = "20210529172016";
        Set<String> removePts = Sets.newHashSet(minPt1, minPt2);

        Statement showPartitionsStatement = Mockito.mock(Statement.class, Mockito.withSettings().name("showPartitionsStatement"));
        ResultSet showPartitionsResult = Mockito.mock(ResultSet.class, Mockito.withSettings().name("showPartitionsResultSet"));
        Mockito.when(showPartitionsStatement.executeQuery("show partitions testdb.order")).thenReturn(showPartitionsResult);
        Mockito.when(showPartitionsResult.next())
                .thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(showPartitionsResult.getString(1))
                .thenReturn("pt=" + minPt1, "pt=" + minPt2, "pt=" + retainPt1, "pt=" + retainPt2);
        showPartitionsResult.close();
        showPartitionsStatement.close();

        //====================================
        Statement dropPtStatement2 = Mockito.mock(Statement.class, Mockito.withSettings().name("dropStatement_" + minPt2));
        Mockito.when(dropPtStatement2.execute("alter table testdb.order drop partition (  pt = '" + minPt2 + "' )")).thenReturn(true);
        dropPtStatement2.close();


        Statement dropPtStatement1 = Mockito.mock(Statement.class, Mockito.withSettings().name("dropStatement_" + minPt1));
        Mockito.when(dropPtStatement1.execute("alter table testdb.order drop partition (  pt = '" + minPt1 + "' )")).thenReturn(true);
        dropPtStatement1.close();


        int partitionRetainNum = 2;
        EntityName tabOrder = EntityName.create(dbName, tabName);
        //hiveConn.close();
        // hiveConn.createStatement() 原为队列语义(依次返回), Mockito 以链式等价
        Mockito.when(hiveConn.createStatement()).thenReturn(showDBStatment, showTabsStatement
                , showPartitionsStatement, dropPtStatement2, dropPtStatement1);
        List<FSHistoryFileUtils.PathInfo> deletePts =
                (new HiveRemoveHistoryDataTask(MREngine.HIVE,fileSystem, sourceMeta)).dropHistoryHiveTable(tabOrder, hiveConn, partitionRetainNum);

        assertEquals(2, deletePts.size());
        deletePts.forEach((pt) -> {
            assertTrue("removed partition shall exist:" + pt.getPathName(), removePts.contains(pt.getPathName()));
        });
        Mockito.verify(dropPtStatement2).execute("alter table testdb.order drop partition (  pt = '" + minPt2 + "' )");
        Mockito.verify(dropPtStatement1).execute("alter table testdb.order drop partition (  pt = '" + minPt1 + "' )");
        Mockito.verify(showDBStatment).close();
        Mockito.verify(resultSet).close();
        Mockito.verify(tabsResult).close();
        Mockito.verify(showPartitionsResult).close();
        Mockito.verify(showPartitionsStatement).close();
        Mockito.verify(dropPtStatement2).close();
        Mockito.verify(dropPtStatement1).close();
    }
}
