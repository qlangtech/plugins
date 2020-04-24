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

import com.qlangtech.tis.dump.DumpTable;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
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
    public static void bindHiveTables(ITISFileSystemFactory fileSystem, Set<DumpTable> hiveTables, String timestamp, ITaskContext context) {
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
