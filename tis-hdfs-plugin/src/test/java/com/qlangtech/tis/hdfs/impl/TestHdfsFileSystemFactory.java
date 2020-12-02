package com.qlangtech.tis.hdfs.impl;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.offline.FileSystemFactory;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2020-11-24 17:50
 **/
public class TestHdfsFileSystemFactory extends TestCase {
    public void testPluginGet() {
        List<Descriptor<FileSystemFactory>> descList
                = TIS.get().getDescriptorList(FileSystemFactory.class);
        assertNotNull(descList);
        assertEquals(1, descList.size());
    }
}
