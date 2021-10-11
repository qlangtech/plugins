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
package com.qlangtech.tis.fullbuild.taskflow.hive;

import com.qlangtech.tis.fs.IFs2Table;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.ITaskFactory;
import com.qlangtech.tis.fullbuild.taskflow.ITemplateContext;
import com.qlangtech.tis.plugin.datax.MREngine;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年11月2日 上午10:26:18
 */
public class HiveTaskFactory implements ITaskFactory {

    // private final HiveDBUtils hiveDBHelper;
    private final IPrimaryTabFinder erRules;

    private final ITISFileSystem fileSystem;

    public HiveTaskFactory(IPrimaryTabFinder erRules, ITISFileSystemFactory fileSystem) {
        super();
        // this.hiveDBHelper = HiveDBUtils.getInstance();
        this.erRules = erRules;
        this.fileSystem = fileSystem.getFileSystem();
    }


    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, boolean isFinalNode, ITemplateContext tplContext
            , ITaskContext taskContext, IFs2Table fs2Table, IJoinTaskStatus joinTaskStatus) {
        if (fileSystem == null) {
            throw new IllegalStateException("filesystem can not be null");
        }
        JoinHiveTask task = new JoinHiveTask(nodeMeta, isFinalNode, this.erRules, joinTaskStatus, fileSystem, fs2Table, MREngine.HIVE);
        task.setContext(tplContext, taskContext);
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
