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
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.hive.HiveColumn;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
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
import java.util.stream.Collectors;

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
    private ITISFileSystem fileSystem;

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
            public boolean callback(ResultSet result) throws Exception {
                tables.add(result.getString(1));
                return true;
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
        HiveDBUtils.query(conn, "show tables from " + dbName, result -> tables.add(result.getString(2)));
        return tables;
    }

    /**
     * Reference:https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTableCreate/Drop/TruncateTable
     *
     * @param conn
     * @param table
     * @param cols
     * @param sqlCommandTailAppend
     * @throws Exception
     */
    public void createHiveTableAndBindPartition(Connection conn, EntityName table, List<HiveColumn> cols, SQLCommandTailAppend sqlCommandTailAppend) throws Exception {


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
        HiveDBUtils.executeNoLog(conn, "CREATE DATABASE IF NOT EXISTS " + table.getDbName() + "");
        hiveSQl.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + table.getFullName() + " (\n");
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
        //hiveSQl.append(") COMMENT 'tis_tmp_" + table + "' PARTITIONED BY(pt string,pmod string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES " + "TERMINATED BY '\\n' NULL DEFINED AS '' STORED AS TEXTFILE ");
        hiveSQl.append(") COMMENT 'tis_tmp_" + table + "' PARTITIONED BY(pt string,pmod string)   ");
        hiveSQl.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' with SERDEPROPERTIES ('serialization.null.format'='','line.delim' ='\n','field.delim'='\t')");
        hiveSQl.append("STORED AS TEXTFILE");
        sqlCommandTailAppend.append(hiveSQl);
        log.info(hiveSQl.toString());
        HiveDBUtils.executeNoLog(conn, hiveSQl.toString());
    }

    private static String getHiveType(int type) {
        // java.sql.Types
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTableCreate/Drop/TruncateTable
        switch (type) {
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
            case TIMESTAMP:
                return "TIMESTAMP";
            case DATE:
                return "DATE";
            default:
                return HiveColumn.HIVE_TYPE_STRING;
        }
    }

    /**
     * 和hdfs上已经导入的数据进行绑定
     *
     * @param hiveTables
     * @throws Exception
     */
    public void bindHiveTables(ITISFileSystem fileSystem, Set<EntityName> hiveTables, ITaskContext context) throws Exception {
        Connection conn = null;
        try {
            conn = context.getObj();
            for (EntityName hiveTable : hiveTables) {
                String hivePath = hiveTable.getNameWithPath();
                final List<String> tables = getExistTables(conn, hiveTable.getDbName());
                List<HiveColumn> columns = getColumns(hivePath, timestamp);
                if (tables.contains(hiveTable.getTableName())) {
                    if (!isTableSame(conn, columns, hiveTable)) {
                        // 原表有改动，需要把表drop掉
                        log.info("table:" + hiveTable.getTableName() + " exist but table col metadta has been changed");
                        HiveDBUtils.execute(conn, "DROP TABLE " + hiveTable);
                        this.createHiveTableAndBindPartition(conn, columns, hiveTable);
                        //return;
                    } else {
                        log.info("table:" + hiveTable.getTableName() + " exist will bind new partition");
                    }
                } else {
                    log.info("table not exist,will create now:" + hiveTable.getTableName() + ",exist table:["
                            + tables.stream().collect(Collectors.joining(",")) + "]");
                    this.createHiveTableAndBindPartition(conn, columns, hiveTable);
                    //return;
                }

                // 生成 hive partitiion
                this.bindPartition(conn, hivePath, hiveTable, 0);
            }
        } finally {
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
    }

    public static boolean isTableSame(Connection conn, List<HiveColumn> columns, EntityName tableName) throws Exception {
        boolean isTableSame;
        final StringBuffer errMsg = new StringBuffer();
        final StringBuffer equalsCols = new StringBuffer("compar equals:");
        final AtomicBoolean compareOver = new AtomicBoolean(false);
        HiveDBUtils.query(conn, "desc " + tableName, new HiveDBUtils.ResultProcess() {

            int index = 0;

            @Override
            public boolean callback(ResultSet result) throws Exception {
                if (errMsg.length() > 0) {
                    return false;
                }
                final String keyName = result.getString(1);
                if (compareOver.get() || (StringUtils.isBlank(keyName) && compareOver.compareAndSet(false, true))) {
                    // 所有列都解析完成
                    return false;
                }
                if (omitKeys.contains(keyName)) {
                    // 忽略pt和pmod
                    return true;
                }
                if (StringUtils.startsWith(keyName, "#")) {
                    return false;
                }
                if (index > (columns.size() - 1)) {
                    errMsg.append("create table " + tableName + " col:[" + keyName + "] is not exist");
                    return false;
                }
                HiveColumn column = columns.get(index++);
                if (column.getIndex() != (index - 1)) {
                    throw new IllegalStateException("col:" + column.getName() + " index shall be " + (index - 1) + " but is " + column.getIndex());
                }
                if (!StringUtils.equals(keyName, column.getName())) {
                    errMsg.append("target table keyName[" + index + "]:" + keyName + " is not equal with source table col:" + column.getName());
                    return false;
                } else {
                    equalsCols.append(keyName).append(",");
                }
                return true;
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
    public static boolean isTableExists(Connection connection, EntityName dumpTable) throws Exception {
        // 判断表是否存在
        if (!isDBExists(connection, dumpTable.getDbName())) {
            // DB都不存在，table肯定就不存在啦
            log.debug("dumpTable'DB is not exist:{}", dumpTable);
            return false;
        }
        final List<String> tables = new ArrayList<>();
//        +-----------+---------------+--------------+--+
//        | database  |   tableName   | isTemporary  |
//        +-----------+---------------+--------------+--+
//        | order     | totalpayinfo  | false        |
//        +-----------+---------------+--------------+--+
        HiveDBUtils.query(connection, "show tables in " + dumpTable.getDbName()
                , result -> tables.add(result.getString(2)));
        boolean contain = tables.contains(dumpTable.getTableName());
        if (!contain) {
            log.debug("table:{} is not exist in[{}]", dumpTable.getTableName()
                    , tables.stream().collect(Collectors.joining(",")));
        }
        return contain;
    }

    private static boolean isDBExists(Connection connection, String dbName) throws Exception {
        AtomicBoolean dbExist = new AtomicBoolean(false);
        HiveDBUtils.query(connection, "show databases", result -> {
            if (StringUtils.equals(result.getString(1), dbName)) {
                dbExist.set(true);
            }
            return true;
        });
        return dbExist.get();
    }

    /**
     * 创建表分区
     *
     * @param hivePath
     * @throws Exception
     */
    public void bindPartition(Connection conn, String hivePath, EntityName table, int startIndex) throws Exception {

        visitSubPmodPath(hivePath, startIndex, (pmod, path) -> {
            String sql = "alter table " + table + " add if not exists partition(pt='"
                    + timestamp + "',pmod='" + pmod + "') location '" + path.toString() + "'";
            log.info(sql);
            HiveDBUtils.executeNoLog(conn, sql);
            return true;
        });


//        String sql = null;
//        ITISFileSystem fs = this.fileSystem.getFileSystem();
//        IPath path = null;
//        while (true) {
//            path = fs.getPath(this.fileSystem.getRootDir() + "/" + hivePath + "/all/" + timestamp + "/" + (startIndex));
//            if (!fs.exists(path)) {
//                break;
//            }
//            sql = "alter table " + table + " add if not exists partition(pt='" + timestamp + "',pmod='" + startIndex + "') location '" + path.toString() + "'";
//            log.info(sql);
//            HiveDBUtils.executeNoLog(conn, sql);
//            startIndex++;
//        }
    }

    private IPath visitSubPmodPath(String hivePath, int startIndex, FSPathVisitor pathVisitor) throws Exception {

        // String sql = null;
        ITISFileSystem fs = this.fileSystem;
        IPath path = null;

        while (true) {
            path = fs.getPath(this.fileSystem.getRootDir() + "/" + hivePath + "/all/" + timestamp + "/" + (startIndex));
            if (!fs.exists(path)) {
                return path;
            }
            if (!pathVisitor.process(startIndex, path)) { return path;}
//            sql = "alter table " + table + " add if not exists partition(pt='" + timestamp + "',pmod='" + startIndex + "') location '" + path.toString() + "'";
//            log.info(sql);
//            HiveDBUtils.executeNoLog(conn, sql);
            startIndex++;
        }
    }

    private interface FSPathVisitor {
        boolean process(int pmod, IPath path) throws Exception;
    }

    @SuppressWarnings("all")
    private List<HiveColumn> getColumns(String hivePath, String timestamp) throws IOException {
        InputStream input = null;
        List<HiveColumn> cols = new ArrayList<>();
        try {
            ITISFileSystem fs = this.fileSystem;
            input = fs.open(fs.getPath(fs.getRootDir() + "/" + hivePath + "/all/" + timestamp + "/" + ColumnMetaData.KEY_COLS_METADATA));
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

    private void createHiveTableAndBindPartition(Connection conn, List<HiveColumn> columns, EntityName tableName) throws Exception {
        createHiveTableAndBindPartition(conn, tableName, columns, (hiveSQl) -> {
                    final String hivePath = tableName.getNameWithPath();
                    int startIndex = 0;
                    //final StringBuffer tableLocation = new StringBuffer();
                    AtomicBoolean hasSubPmod = new AtomicBoolean(false);
                    IPath p = this.visitSubPmodPath(hivePath, startIndex, (pmod, path) -> {
                        // hiveSQl.append(" partition(pt='" + timestamp + "',pmod='" + pmod + "') location '" + path.toString() + "'");

                        hiveSQl.append(" location '" + path.toString() + "'");

                        hasSubPmod.set(true);
                        return false;
                    });

                    if (!hasSubPmod.get()) {
                        throw new IllegalStateException("dump table:" + tableName.getFullName() + " has any valid of relevant fs in timestamp path:" + p);
                    }
                }
        );
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
    public void setFileSystem(ITISFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public interface SQLCommandTailAppend {
        public void append(StringBuffer hiveSQl) throws Exception;
    }
}
