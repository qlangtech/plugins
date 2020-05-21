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
package com.qlangtech.tis.fullbuild.taskflow.hive;

import com.qlangtech.tis.fs.IFs2Table;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.ITaskFactory;
import com.qlangtech.tis.fullbuild.taskflow.ITemplateContext;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.ERRules;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年11月2日 上午10:26:18
 */
public class HiveTaskFactory implements ITaskFactory {

    // private final HiveDBUtils hiveDBHelper;
    private final ERRules erRules;

    private final ITISFileSystemFactory fileSystem;

    public HiveTaskFactory(ERRules erRules, ITISFileSystemFactory fileSystem) {
        super();
        // this.hiveDBHelper = HiveDBUtils.getInstance();
        this.erRules = erRules;
        this.fileSystem = fileSystem;
    }

    // private static final String HIVE_JDBC_CONNECTION_KEY = "hive_jdbc_connection_key";
    // public void startTaskInitialize(ITemplateContext tplContext) {
    // tplContext.putContextValue(HIVE_JDBC_CONNECTION_KEY, HiveDBUtils.getInstance().createConnection());
    // }
    // 
    // public static Connection getConnection(ITemplateContext templateContext) {
    // Connection conn = templateContext.getContextValue(HIVE_JDBC_CONNECTION_KEY);
    // if (conn == null) {
    // throw new IllegalStateException("conn can not be null");
    // }
    // return conn;
    // }
    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, boolean isFinalNode, ITemplateContext tplContext, ITaskContext taskContext, IFs2Table fs2Table, IJoinTaskStatus joinTaskStatus) {
        if (fileSystem == null) {
            throw new IllegalStateException("filesystem can not be null");
        }
        JoinHiveTask task = new JoinHiveTask(nodeMeta, isFinalNode, this.erRules, joinTaskStatus, fileSystem, fs2Table);
        task.setContext(tplContext, taskContext);
        // task.setHiveDBHelper(hiveDBHelper);
        return task;
    }
    // @Override
    // public void postReleaseTask(ITemplateContext tplContext) {
    // 
    // Connection conn = getConnection(tplContext);
    // try {
    // conn.close();
    // } catch (Exception e) {
    // }
    // }
}
