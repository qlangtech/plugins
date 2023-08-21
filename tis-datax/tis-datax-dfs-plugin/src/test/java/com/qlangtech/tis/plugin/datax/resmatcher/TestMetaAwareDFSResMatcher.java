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

package com.qlangtech.tis.plugin.datax.resmatcher;

import com.alibaba.datax.plugin.ftp.common.FtpHelper;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.test.TISEasyMock;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-18 15:13
 **/
public class TestMetaAwareDFSResMatcher implements TISEasyMock {
    private static final EntityName table = EntityName.parse("orderdetail");
    private static final String rootPath = "/user/admin";
    private static final String rootPathWithTabName = IPath.pathConcat("/user/admin", table.getTableName());

    @Before
    public void clean() {
        this.clearMocks();
    }

    @Test
    public void testGetTableMetadata() throws Exception {
        MetaAwareDFSResMatcher resMatcher = new MetaAwareDFSResMatcher();

        IDFSReader dfsReader = mock("dfsReader", IDFSReader.class);
        testGetTableMetadataWithRootPath(rootPath, resMatcher, dfsReader);
    }

    /**
     * 测试目的，为了MetaAwareDFSResMatcher.getDFSFileMetaData() 执行逻辑的正确性
     * 当在 TDFSLinker 路径属性(path)中带上 table作为后缀时
     *
     * @throws Exception
     * @see TDFSLinker
     * @see MetaAwareDFSResMatcher .getDFSFileMetaData()
     */
    @Test
    public void testGetTableMetadataByPathWithTabName() throws Exception {
        MetaAwareDFSResMatcher resMatcher = new MetaAwareDFSResMatcher();

        IDFSReader dfsReader = mock("dfsReader", IDFSReader.class);
        testGetTableMetadataWithRootPath(rootPathWithTabName, resMatcher, dfsReader);
    }

    private void testGetTableMetadataWithRootPath(String rootPath, MetaAwareDFSResMatcher resMatcher, IDFSReader dfsReader) throws TableNotFoundException {
        TDFSLinker dfsLinker = new MockTDFSLinker(rootPath);

        EasyMock.expect(dfsReader.getDfsLinker()).andReturn(dfsLinker).anyTimes();


        this.replay();
        List<ColumnMetaData> cols = resMatcher.getTableMetadata(dfsReader, table);
        Assert.assertNotNull("cols can not be null", cols);
        Assert.assertEquals(59, cols.size());
        this.verifyAll();
    }


    private static class MockTDFSLinker extends TDFSLinker {


        public MockTDFSLinker(String rootPath) {
            this.path = rootPath;
        }

        @Override
        public ITDFSSession createTdfsSession(Integer timeout) {
            return createTdfsSession();
        }

        @Override
        public ITDFSSession createTdfsSession() {
            return new MockDFSSession(this.path);
        }

        @Override
        public <T> T useTdfsSession(TDFSSessionVisitor<T> tdfsSession) {
            try {
                return tdfsSession.accept(createTdfsSession());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class MockDFSSession implements ITDFSSession {

        private final String rootPath;

        public MockDFSSession(String rootPath) {
            this.rootPath = rootPath;
        }

        @Override
        public String getRootPath() {
            return rootPath;
        }

        @Override
        public boolean isDirExist(String directoryPath) {
            return false;
        }

        @Override
        public void mkDirRecursive(String directoryPath) {

        }

        @Override
        public Set<String> getAllFilesInDir(String path, String fileName) {
            return null;
        }

        @Override
        public void deleteFiles(Set<String> filesToDelete) {

        }

        @Override
        public HashSet<Res> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel) {
            return null;
        }

        @Override
        public HashSet<Res> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
            return null;
        }

        @Override
        public OutputStream getOutputStream(String filePath, boolean append) {
            return null;
        }

        @Override
        public InputStream getInputStream(String filePath) {
            Assert.assertEquals(IPath.pathConcat(TestMetaAwareDFSResMatcher.rootPath, table.getTableName(), FtpHelper.KEY_META_FILE), filePath);
            return IOUtils.toInputStream(
                    com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(TestMetaAwareDFSResMatcher.class
                            , "instancedetail_meta.json"), TisUTF8.get());
        }

        @Override
        public void close() throws Exception {

        }
    }

}
