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

package org.apache.solr.handler.admin;

import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.response.SolrQueryResponse;

import java.io.File;
import java.io.IOException;

/**
 * @author: baisui 百岁
 * @create: 2021-03-09 09:15
 **/
public class MockTisCoreAdminHandler extends TisCoreAdminHandler {
    public MockTisCoreAdminHandler(CoreContainer coreContainer) {
        super(coreContainer);
    }

    //  protected void downloadIndexFile2IndexDir(TaskObject taskObj, long hdfsTimeStamp, String solrCoreName, final File indexDir, final SolrQueryResponse rsp, String taskId)
    @Override
    public void downloadIndexFile2IndexDir(TaskObject taskObj, long hdfsTimeStamp, String solrCoreName, File indexDir, SolrQueryResponse rsp, String taskId) {
        super.downloadIndexFile2IndexDir(taskObj, hdfsTimeStamp, solrCoreName, indexDir, rsp, taskId);
    }

//    public void addCompletedTask(String taskId) {
//        TaskObject taskObject = new TaskObject(taskId);
//        this.addTask(COMPLETED, taskObject, false);
//    }

    public void addRunningTask(String taskId) {
        TaskObject taskObject = new TaskObject(taskId);
        this.addTask(RUNNING, taskObject, false);
    }

    @Override
    protected Directory createChildIndexDirectory(ITISFileSystem filesystem, IPath path) throws IOException {
        //return new TisHdfsDirectory(path, filesystem);
        File f = path.unwrap(File.class);
        return new MMapDirectory(f.toPath(), NoLockFactory.INSTANCE);
    }


    void addTask(String type, TaskObject o, boolean limit) {
        super.addTask(type, o, limit);
    }

}
