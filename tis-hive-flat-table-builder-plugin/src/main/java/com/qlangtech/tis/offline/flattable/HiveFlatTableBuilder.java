/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.offline.flattable;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.dump.INameWithPathGetter;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.FSHistoryFileUtils;
import com.qlangtech.tis.fs.IFs2Table;
import com.qlangtech.tis.fs.ITableBuildTask;
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
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import org.apache.commons.dbcp.DelegatingConnection;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/*
 * HIVE 宽表构建task
 * @create: 2020-04-03 12:12
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class HiveFlatTableBuilder extends FlatTableBuilder {

    public static final String KEY_FIELD_NAME_FS_NAME = "fsName";
    public static final String KEY_HIVE_ADDRESS = "hiveAddress";
    public static final String KEY_DB_NAME = "dbName";

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.host})
    public String // "jdbc:hive2://10.1.5.68:10000/tis";
            hiveAddress;

    @FormField(ordinal = 2, validate = {Validator.require, Validator.identity})
    public String // "jdbc:hive2://10.1.5.68:10000/tis";
            dbName;

    @FormField(ordinal = 3, validate = {Validator.require, Validator.identity}, type = FormFieldType.SELECTABLE)
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

    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, boolean isFinalNode
            , ITemplateContext tplContext, ITaskContext taskContext, IFs2Table fs2Table, IJoinTaskStatus joinTaskStatus) {
        HiveTaskFactory taskFactory = getTaskFactory(tplContext);
        return taskFactory.createTask(nodeMeta, isFinalNode, tplContext, taskContext, fs2Table, joinTaskStatus);
    }

    private HiveTaskFactory getTaskFactory(ITemplateContext tplContext) {
        IPrimaryTabFinder erRules = tplContext.getExecContext().getAttribute(IFullBuildContext.KEY_ER_RULES);
        Objects.requireNonNull(erRules, "erRule can not be null");
        Objects.requireNonNull(getFs(), "join relevant FS can not be null");
        this.taskFactory = new HiveTaskFactory(erRules, getFs());
        return taskFactory;
    }

    @Override
    public void startTask(ITableBuildTask dumpTask) {
        final Connection conn = getConnection();
        final DelegatingConnection delegate = new DelegatingConnection(conn) {
            @Override
            public void close() throws SQLException {
                throw new UnsupportedOperationException("in exec phrase close is not supported");
            }
        };
        ITaskContext context = new ITaskContext() {
            @Override
            public Connection getObj() {
                return delegate;
            }
        };
        try {
            dumpTask.process(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
            }
        }
    }

    private Connection getConnection() {
        try {
            return HiveDBUtils.getInstance(this.hiveAddress, this.dbName).createConnection();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


    public String getJoinTableStorePath(String user, INameWithPathGetter pathGetter) {
        return FSHistoryFileUtils.getJoinTableStorePath(user, pathGetter);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<FlatTableBuilder> {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME_FS_NAME, () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }

        @Override
        public String getDisplayName() {
            return "hive";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return validateHiveAvailable(msgHandler, context, postFormVals);
        }
    }

    public static boolean validateHiveAvailable(IControlMsgHandler msgHandler, Context context, Descriptor.PostFormVals postFormVals) {
        String hiveAddress = postFormVals.getField(KEY_HIVE_ADDRESS);
        String dbName = postFormVals.getField(KEY_DB_NAME);

        Connection conn = null;
        try {
            conn = HiveDBUtils.getInstance(hiveAddress, dbName).createConnection();
        } catch (Throwable e) {
            Throwable[] throwables = ExceptionUtils.getThrowables(e);
            for (Throwable t : throwables) {
                if (StringUtils.indexOf(t.getMessage(), "refused") > -1) {
                    msgHandler.addFieldError(context, KEY_HIVE_ADDRESS, "连接地址不可用，请确保连接Hive服务地址可用");
                    return false;
                }
                if (StringUtils.indexOf(t.getMessage(), "NoSuchDatabaseException") > -1) {
                    msgHandler.addFieldError(context, KEY_DB_NAME, "dbName:" + dbName + " is not exist ,please create");
                    return false;
                }
            }
            throw e;
        } finally {
            try {
                conn.close();
            } catch (Throwable e) {}
        }
        return true;
    }
}
