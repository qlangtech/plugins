/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import org.apache.commons.lang.StringUtils;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-07 09:20
 **/
public class DataXDFSSelectTableReaderContext extends DataXDFSReaderContext {
   // private final ISelectedTab tab;
    private final int currentIndex;

    public DataXDFSSelectTableReaderContext(IDFSReader reader, ISelectedTab tab, int currentIndex) {
        super(reader,tab);
       // this.tab = tab;
        this.currentIndex = currentIndex;
    }

    public static String buildMetaAwareDFSTargetPath(TDFSLinker dfsLinker , String tabName) {
        String dfsPath = IPath.pathConcat(dfsLinker.getRootPath(), tabName);
        String[] pathSplit = StringUtils.split(dfsLinker.getRootPath(), File.separator);
        if (pathSplit.length > 0) {
            /**
             * TDFSLinker 的path 设置 为 meta.json 的直接父目录的话，则不需要再在path 添加表名称了，不然路径不对了
             */
            if (pathSplit[pathSplit.length - 1].equals(tabName)) {
                dfsPath = dfsLinker.getRootPath();
            }
        }
        return dfsPath;
    }

    @Override
    public String getReaderContextId() {
        return FTP_TASK;
    }

    @Override
    public String getPath() {

        return buildMetaAwareDFSTargetPath(this.reader.getDfsLinker(), tab.getName());
//        String path = this.reader.getDfsLinker().getRootPath();
//        return IPath.pathConcat(path,tab.getName());
//        boolean endWithSlash = StringUtils.endsWith(path, String.valueOf(IOUtils.DIR_SEPARATOR));
//        return path + (endWithSlash ? StringUtils.EMPTY : IOUtils.DIR_SEPARATOR) + tab.getName();
    }

    @Override
    public String getTaskName() {
        return tab.getName() + "_" + currentIndex;
    }


//    @Override
//    public String getColumn() {
//        JSONArray cols = new JSONArray();
//        JSONObject o = null;
//        int index = 0;
//        for (CMeta col : tab.getCols()) {
//            o = new JSONObject();
//            o.put(ParseColsResult.KEY_TYPE, col.getType().getCollapse().getLiteria());
//            o.put(ParseColsResult.KEY_INDEX, index++);
//            cols.add(o);
//        }
//        return JsonUtil.toString(cols);
//    }

    @Override
    public String getSourceTableName() {
        return tab.getName();
    }
}
