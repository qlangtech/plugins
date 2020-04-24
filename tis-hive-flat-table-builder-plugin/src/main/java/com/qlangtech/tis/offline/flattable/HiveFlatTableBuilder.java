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
package com.qlangtech.tis.offline.flattable;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.dump.INameWithPathGetter;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.dump.hive.HiveRemoveHistoryDataTask;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITableBuildTask;
import com.qlangtech.tis.fs.IFs2Table;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.ITemplateContext;
import com.qlangtech.tis.fullbuild.taskflow.hive.HiveTaskFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.offline.FlatTableBuilder;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.ERRules;
import java.sql.Connection;
import java.util.Objects;

/*
 * HIVE 宽表构建task
 * @create: 2020-04-03 12:12
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class HiveFlatTableBuilder extends FlatTableBuilder {

    public static final String KEY_FIELD_NAME = "fsName";

    @FormField(ordinal = 0, validate = { Validator.require, Validator.identity })
    public String name;

    @FormField(ordinal = 1, validate = { Validator.require, Validator.host })
    public String // "jdbc:hive2://10.1.5.68:10000/tis";
    hiveAddress;

    @FormField(ordinal = 2, validate = { Validator.require, Validator.identity })
    public String // "jdbc:hive2://10.1.5.68:10000/tis";
    dbName;

    @FormField(ordinal = 3, validate = { Validator.require, Validator.identity }, type = FormFieldType.SELECTABLE)
    public String fsName;

    private FileSystemFactory fileSystem;

    private FileSystemFactory getFs() {
        if (fileSystem == null) {
            this.fileSystem = FileSystemFactory.getFsFactory(fsName);
        }
        Objects.requireNonNull(this.fileSystem, "fileSystem has not be initialized");
        return fileSystem;
    }

    private HiveTaskFactory taskFactory;

    // private HiveRemoveHistoryDataTask removeHistoryDataTask;
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, ITemplateContext tplContext, ITaskContext taskContext, IFs2Table fs2Table, IJoinTaskStatus joinTaskStatus) {
        HiveTaskFactory taskFactory = getTaskFactory(tplContext);
        return taskFactory.createTask(nodeMeta, tplContext, taskContext, fs2Table, joinTaskStatus);
    }

    private HiveTaskFactory getTaskFactory(ITemplateContext tplContext) {
        ERRules erRules = tplContext.joinTaskContext().getAttribute(IFullBuildContext.KEY_ER_RULES);
        Objects.nonNull(erRules);
        Objects.nonNull(getFs());
        this.taskFactory = new HiveTaskFactory(erRules.getTabFieldProcessorMap(), getFs());
        return taskFactory;
    }

    @Override
    public void startTask(ITableBuildTask dumpTask) {
        final Connection conn = HiveDBUtils.getInstance(this.hiveAddress, this.dbName).createConnection();
        ITaskContext context = new ITaskContext() {

            @Override
            public Connection getObj() {
                return conn;
            }
        };
        try {
            dumpTask.process(context);
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
            }
        }
    }

    public String getJoinTableStorePath(String user, INameWithPathGetter pathGetter) {
        return HiveRemoveHistoryDataTask.getJoinTableStorePath(user, pathGetter);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<FlatTableBuilder> {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }
        @Override
        public String getDisplayName() {
            return "hive";
        }
    }
}
