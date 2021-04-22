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

import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.util.Objects;
import java.util.Set;

/*
 * 将hdfs上的数据和hive database中的表绑定
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年12月15日 下午4:49:42
 */
public class BindHiveTableTool {

    // private static final Logger logger = LoggerFactory
    // .getLogger(BindHiveTableTool.class);
    public static void bindHiveTables(ITISFileSystem fileSystem, Set<EntityName> hiveTables, String timestamp, ITaskContext context) {
        Objects.nonNull(fileSystem);
        // Objects.nonNull(userName);
        Objects.nonNull(timestamp);
        try {
            // StringBuffer tabs = new StringBuffer();
            // for (String tab : hiveTables) {
            // tabs.append(tab).append(",");
            // }
            // final String user = userName;// System.getProperty("user.name");
            // ▼▼▼▼ bind hive table task
            // logger.info("start hive table bind,tabs:" + tabs.toString());
            // Dump 任务结束,开始绑定hive partition
            HiveTableBuilder hiveTableBuilder = new HiveTableBuilder(timestamp);
            // hiveTableBuilder.setHiveDbHeler(HiveDBUtils.getInstance());
            hiveTableBuilder.setFileSystem(fileSystem);
            hiveTableBuilder.bindHiveTables(fileSystem, hiveTables, context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
