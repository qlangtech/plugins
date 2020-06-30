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

import com.qlangtech.tis.dump.hive.HiveRemoveHistoryDataTask;
import com.qlangtech.tis.dump.hive.HiveRemoveHistoryDataTask.PathInfo;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.order.dump.task.ITableDumpConstant;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年11月26日 上午11:54:31
 */
class RemoveJoinHistoryDataTask {

    private RemoveJoinHistoryDataTask() {
    }

    /**
     * 删除宽表历史数据
     *
     * @param chainContext
     * @throws Exception
     */
    void deleteHistoryJoinTable(EntityName dumpTable, IJoinTaskContext chainContext, ITISFileSystemFactory fileSys) throws Exception {
        if (chainContext == null) {
            throw new IllegalArgumentException("param: execContext can not be null");
        }
        final String path = HiveRemoveHistoryDataTask.getJoinTableStorePath(fileSys.getRootDir(), dumpTable).replaceAll("\\.", Path.SEPARATOR);
        if (fileSys == null) {
            throw new IllegalStateException("fileSys can not be null");
        }
        ITISFileSystem fs = fileSys.getFileSystem();
        // new Path(hdfsPath);
        IPath parent = fs.getPath(path);
        if (!fs.exists(parent)) {
            return;
        }
        List<IPathInfo> child = fs.listChildren(parent);
        PathInfo pathinfo;
        List<PathInfo> timestampList = new ArrayList<>();
        Matcher matcher;
        for (IPathInfo c : child) {
            matcher = HiveRemoveHistoryDataTask.DATE_PATTERN.matcher(c.getPath().getName());
            if (matcher.find()) {
                pathinfo = new PathInfo();
                pathinfo.setPathName(c.getPath().getName());
                pathinfo.setTimeStamp(Long.parseLong(matcher.group()));
                timestampList.add(pathinfo);
            }
        }
        HiveRemoveHistoryDataTask.deleteOldHdfsfile(fs, parent, timestampList, ITableDumpConstant.MAX_PARTITION_SAVE);
    }
}
