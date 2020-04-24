/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.dump.hive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.dump.DumpTable;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.hive.HiveColumn;
import com.qlangtech.tis.manage.common.TisUTF8;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.sql.Types.*;

/*
 * 在hive中生成新表，和在新表上创建创建Partition
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年10月31日 下午3:43:14
 */
public class HiveTableBuilder {

    private static final Logger log = LoggerFactory.getLogger(HiveTableBuilder.class);

    private final String timestamp;

    // private final String user;
    // private HiveDBUtils hiveDbHeler;
    private ITISFileSystemFactory fileSystem;

    private static HashSet<String> omitKeys = new HashSet<String>();

    static {
        omitKeys.add(IDumpTable.PARTITION_PT);
        omitKeys.add(IDumpTable.PARTITION_PMOD);
    }

    public HiveTableBuilder(String timestamp) {
        super();
        if (StringUtils.isEmpty(timestamp)) {
            throw new IllegalArgumentException("param timestamp can not be null");
        }
        this.timestamp = timestamp;
    // this.user = user;
    }

    /**
     * @param
     * @return
     * @throws Exception
     */
    public static List<String> getExistTables(Connection conn) throws Exception {
        final List<String> tables = new ArrayList<>();
        HiveDBUtils.query(conn, "show tables", new HiveDBUtils.ResultProcess() {

            @Override
            public void callback(ResultSet result) throws Exception {
                tables.add(result.getString(1));
            }
        });
        return tables;
    }

    /**
     * @param
     * @return
     * @throws Exception
     */
    public static List<String> getExistTables(Connection conn, String dbName) throws Exception {
        final List<String> tables = new ArrayList<>();
        if (!isDBExists(conn, dbName)) {
            // DB都不存在，table肯定就不存在啦
            return tables;
        }
        HiveDBUtils.query(conn, "show tables from " + dbName, result -> tables.add(result.getString(1)));
        return tables;
    }

    /**
     * @param conn
     * @param table
     * @param cols
     * @param sqlCommandTailAppend
     * @throws Exception
     */
    public static void createHiveTable(Connection conn, DumpTable table, List<HiveColumn> cols, SQLCommandTailAppend sqlCommandTailAppend) throws Exception {
        int maxColLength = 0;
        int tmpLength = 0;
        for (HiveColumn c : cols) {
            tmpLength = StringUtils.length(c.getName());
            if (tmpLength < 1) {
                throw new IllegalStateException("col name length can not small than 1,cols size:" + cols.size());
            }
            if (tmpLength > maxColLength) {
                maxColLength = tmpLength;
            }
        }
        HiveColumn o = null;
        String colformat = "%-" + (++maxColLength) + "s";
        StringBuffer hiveSQl = new StringBuffer();
        HiveDBUtils.executeNoLog(conn, "CREATE DATABASE IF NOT EXISTS `" + table.getDbName() + "`");
        hiveSQl.append("CREATE EXTERNAL TABLE IF NOT EXISTS `" + table.getFullName() + "` (\n");
        final int colsSize = cols.size();
        for (int i = 0; i < colsSize; i++) {
            o = cols.get(i);
            if (i != o.getIndex()) {
                throw new IllegalStateException("i:" + i + " shall equal with index:" + o.getIndex());
            }
            hiveSQl.append("  ").append("`").append(String.format(colformat, o.getName() + '`')).append(" ").append(o.getType());
            if ((i + 1) < colsSize) {
                hiveSQl.append(",");
            }
            hiveSQl.append("\n");
        }
        hiveSQl.append(") COMMENT 'tis_hive_tmp_" + table + "' PARTITIONED BY(pt string,pmod string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES " + "TERMINATED BY '\\n' NULL DEFINED AS '' STORED AS TEXTFILE");
        sqlCommandTailAppend.append(hiveSQl);
        log.info(hiveSQl.toString());
        HiveDBUtils.executeNoLog(conn, hiveSQl.toString());
    }

    private static String getHiveType(int type) {
        switch(type) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
            case REAL:
            case DOUBLE:
            case NUMERIC:
            case DECIMAL:
                return "DOUBLE";
            default:
                return HiveColumn.HIVE_TYPE_STRING;
        }
    }

    // /**
    // * 构建hive中的表
    // *
    // * @param indexName
    // * @throws Exception
    // */
    // public void createHiveTable(Connection conn, String indexName) throws
    // Exception {
    // buildTableDDL(conn, indexName, timestamp,
    // StringUtils.substringAfter(indexName, "search4"));
    // }
    /**
     * 和hdfs上已经导入的数据进行绑定
     *
     * @param hiveTables
     * @throws Exception
     */
    public void bindHiveTables(ITISFileSystemFactory fileSystem, Set<DumpTable> hiveTables, ITaskContext context) throws Exception {
        Connection conn = null;
        try {
            conn = context.getObj();
            for (DumpTable hiveTable : hiveTables) {
                String hivePath = hiveTable.getNameWithPath();
                final List<String> tables = getExistTables(conn, hiveTable.getDbName());
                List<HiveColumn> columns = getColumns(hivePath, timestamp);
                if (tables.contains(hiveTable.getTableName())) {
                    if (isTableSame(conn, columns, hiveTable)) {
                    // 需要清空原来表数据
                    // HiveRemoveHistoryDataTask hiveHistoryClear = new
                    // HiveRemoveHistoryDataTask(tableName,
                    // userName, fileSystem);
                    // hiveHistoryClear.dropHistoryHiveTable(conn);
                    // 之前已经清理过了
                    } else {
                        // 原表有改动，需要把表drop掉
                        HiveDBUtils.execute(conn, "drop table `" + hiveTable + "`");
                        this.createHiveTable(conn, columns, hiveTable);
                    }
                } else {
                    this.createHiveTable(conn, columns, hiveTable);
                }
                // 生成 hive partitiion
                this.createTablePartition(conn, hivePath, hiveTable);
            }
        } finally {
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
    }

    public static boolean isTableSame(Connection conn, List<HiveColumn> columns, DumpTable tableName) throws Exception {
        boolean isTableSame;
        final StringBuffer errMsg = new StringBuffer();
        final StringBuffer equalsCols = new StringBuffer("compar equals:");
        final AtomicBoolean compareOver = new AtomicBoolean(false);
        HiveDBUtils.query(conn, "desc " + tableName, new HiveDBUtils.ResultProcess() {

            int index = 0;

            @Override
            public void callback(ResultSet result) throws Exception {
                if (errMsg.length() > 0) {
                    return;
                }
                final String keyName = result.getString(1);
                if (compareOver.get() || (StringUtils.isBlank(keyName) && compareOver.compareAndSet(false, true))) {
                    // 所有列都解析完成
                    return;
                }
                if (omitKeys.contains(keyName)) {
                    // 忽略pt和pmod
                    return;
                }
                if (index > (columns.size() - 1)) {
                    errMsg.append("create table " + tableName + " col:" + keyName + " is not exist");
                    return;
                }
                HiveColumn column = columns.get(index++);
                if (column.getIndex() != (index - 1)) {
                    throw new IllegalStateException("col:" + column.getName() + " index shall be " + (index - 1) + " but is " + column.getIndex());
                }
                if (!StringUtils.equals(keyName, column.getName())) {
                    errMsg.append("target tanle keyName:" + keyName + " is not equal with source table col:" + column.getName());
                } else {
                    equalsCols.append(keyName).append(",");
                }
            }
        });
        if (errMsg.length() > 0) {
            isTableSame = false;
            log.warn("create table has been modify,error:" + errMsg);
            log.warn(equalsCols.toString());
        } else {
            // 没有改动，不过需要把元表清空
            isTableSame = true;
        }
        return isTableSame;
    }

    /**
     * 判断表是否存在
     *
     * @param connection
     * @param dumpTable
     * @return
     * @throws Exception
     */
    public static boolean isTableExists(Connection connection, DumpTable dumpTable) throws Exception {
        // 判断表是否存在
        if (!isDBExists(connection, dumpTable.getDbName())) {
            // DB都不存在，table肯定就不存在啦
            return false;
        }
        final List<String> tables = new ArrayList<>();
        HiveDBUtils.query(connection, "show tables in " + dumpTable.getDbName(), result -> tables.add(result.getString(1)));
        return tables.contains(dumpTable.getTableName());
    }

    private static boolean isDBExists(Connection connection, String dbName) throws Exception {
        AtomicBoolean dbExist = new AtomicBoolean(false);
        HiveDBUtils.query(connection, "show databases", result -> {
            if (StringUtils.equals(result.getString(1), dbName)) {
                dbExist.set(true);
            }
        });
        return dbExist.get();
    }

    /**
     * 创建表分区
     *
     * @param hivePath
     * @throws Exception
     */
    public void createTablePartition(Connection conn, String hivePath, DumpTable table) throws Exception {
        int index = 0;
        String sql = null;
        ITISFileSystem fs = this.fileSystem.getFileSystem();
        IPath path = null;
        while (true) {
            path = fs.getPath(this.fileSystem.getRootDir() + "/" + hivePath + "/all/" + timestamp + "/" + (index));
            if (!fs.exists(path)) {
                break;
            }
            sql = "alter table " + table + " add if not exists partition(pt='" + timestamp + "',pmod='" + index + "') location '" + path.toString() + "'";
            log.info(sql);
            HiveDBUtils.executeNoLog(conn, sql);
            index++;
        }
    }

    @SuppressWarnings("all")
    private List<HiveColumn> getColumns(String hivePath, String timestamp) throws IOException {
        InputStream input = null;
        List<HiveColumn> cols = new ArrayList<>();
        try {
            ITISFileSystem fs = this.fileSystem.getFileSystem();
            input = fs.open(fs.getPath(this.fileSystem.getRootDir() + "/" + hivePath + "/all/" + timestamp + "/cols-metadata"));
            // input = fileSystem.open(path);
            String content = IOUtils.toString(input, TisUTF8.getName());
            JSONArray array = (JSONArray) JSON.parse(content);
            for (Object anArray : array) {
                JSONObject o = (JSONObject) anArray;
                HiveColumn col = new HiveColumn();
                col.setName(o.getString("key"));
                col.setIndex(o.getIntValue("index"));
                col.setType(getHiveType(o.getIntValue("type")));
                cols.add(col);
            }
        } finally {
            IOUtils.closeQuietly(input);
        }
        return cols;
    }

    private void createHiveTable(Connection conn, List<HiveColumn> columns, DumpTable tableName) throws Exception {
        createHiveTable(conn, tableName, columns, new SQLCommandTailAppend() {

            @Override
            public void append(StringBuffer hiveSQl) {
            }
        });
    }

    // public HiveDBUtils getHiveDbHeler() {
    // return hiveDbHeler;
    // }
    // 
    // public void setHiveDbHeler(HiveDBUtils hiveDbHeler) {
    // this.hiveDbHeler = hiveDbHeler;
    // }
    // public ITISFileSystem getFileSystem() {
    // return fileSystem;
    // }
    public void setFileSystem(ITISFileSystemFactory fileSystem) {
        this.fileSystem = fileSystem;
    }

    public abstract static class SQLCommandTailAppend {

        public abstract void append(StringBuffer hiveSQl);
    }
}
