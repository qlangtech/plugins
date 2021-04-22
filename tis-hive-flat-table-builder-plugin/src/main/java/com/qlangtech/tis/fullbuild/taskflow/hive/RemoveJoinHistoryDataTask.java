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
package com.qlangtech.tis.fullbuild.taskflow.hive;

import com.qlangtech.tis.fs.FSHistoryFileUtils;
import com.qlangtech.tis.fs.FSHistoryFileUtils.PathInfo;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
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
    public static void deleteHistoryJoinTable(EntityName dumpTable, IJoinTaskContext chainContext, ITISFileSystem fileSys) throws Exception {
        if (chainContext == null) {
            throw new IllegalArgumentException("param: execContext can not be null");
        }
        final String path = FSHistoryFileUtils.getJoinTableStorePath(fileSys.getRootDir(), dumpTable).replaceAll("\\.", Path.SEPARATOR);
        if (fileSys == null) {
            throw new IllegalStateException("fileSys can not be null");
        }
        ITISFileSystem fs = fileSys;
        // new Path(hdfsPath);
        IPath parent = fs.getPath(path);
        if (!fs.exists(parent)) {
            return;
        }
        List<IPathInfo> child = fs.listChildren(parent);
        FSHistoryFileUtils.PathInfo pathinfo;
        List<PathInfo> timestampList = new ArrayList<>();
        Matcher matcher;
        for (IPathInfo c : child) {
            matcher = ITISFileSystem.DATE_PATTERN.matcher(c.getPath().getName());
            if (matcher.find()) {
                pathinfo = new PathInfo();
                pathinfo.setPathName(c.getPath().getName());
                pathinfo.setTimeStamp(Long.parseLong(matcher.group()));
                timestampList.add(pathinfo);
            }
        }
        FSHistoryFileUtils.deleteOldHdfsfile(fs, parent, timestampList, ITableDumpConstant.MAX_PARTITION_SAVE);
    }
}
