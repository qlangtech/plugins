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
package com.qlangtech.tis.hdfs.impl;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.BaiscPluginTest;
import com.qlangtech.tis.plugin.PluginStore;

import java.util.List;

/*
 * @create: 2020-04-11 19:11
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestHdfsFileSystemFactory extends BaiscPluginTest {

    public void testCreate() {
        PluginStore pluginStore = TIS.getPluginStore(FileSystemFactory.class);
        assertNotNull(pluginStore);
        Describable<FileSystemFactory> plugin = pluginStore.getPlugin();
        assertNotNull(plugin);

        FileSystemFactory fsFactory = (FileSystemFactory) plugin;

        ITISFileSystem fileSystem = fsFactory.getFileSystem();

        List<IPathInfo> paths = fileSystem.listChildren(fileSystem.getPath("/"));
        for (IPathInfo i : paths) {
            System.out.println(i.getName());
        }

//        plugin.
//
//        assertTrue("real class:" + plugin.getClass().getName(), plugin instanceof HdfsFileSystemFactory);
//        HdfsFileSystemFactory fsFactory = (HdfsFileSystemFactory) plugin;
//        ITISFileSystem fileSystem = fsFactory.getFileSystem();
//        List<IPathInfo> paths = fileSystem.listChildren(fileSystem.getPath(fsFactory.getRootDir() + "/"));
//        for (IPathInfo i : paths) {
//            System.out.println(i.getName());
//        }
    }
}
