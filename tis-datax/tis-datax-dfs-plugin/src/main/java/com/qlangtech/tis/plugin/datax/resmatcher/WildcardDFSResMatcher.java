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

import com.qlangtech.tis.datax.*;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.AbstractDFSReader;
import com.qlangtech.tis.plugin.datax.DataXDFSReaderContext;
import com.qlangtech.tis.plugin.datax.ParseColsResult;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * dfs 资源名称 查找
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-09 09:56
 **/
public class WildcardDFSResMatcher extends BasicDFSResMatcher {

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.relative_path})
    public String wildcard;

    @Override
    public List<ColumnMetaData> getTableMetadata(IDFSReader dfsReader, EntityName table) throws TableNotFoundException {
        throw new UnsupportedOperationException(WildcardDFSResMatcher.class.getSimpleName() + " is not support");
    }

    /**
     * @param path      value of Key.PATH
     * @param processor
     * @return
     */
    @Override
    public SourceColsMeta getSourceColsMeta(ITDFSSession hdfsSession, Optional<String> entityName, String path, IDataxProcessor processor) {
        TableAliasMapper tabAlias = processor.getTabAlias(null);
        Optional<TableAlias> findMapper = tabAlias.findFirst();
        IDataxProcessor.TableMap tabMapper
                = (IDataxProcessor.TableMap) findMapper.orElseThrow(() -> new NullPointerException("TableAlias can not be null"));
        return new SourceColsMeta(tabMapper.getSourceCols());
    }

    @Override
    public List<ISelectedTab> getSelectedTabs(IDFSReader dfsReader) {

        AbstractDFSReader reader = (AbstractDFSReader) dfsReader;
        if (StringUtils.isEmpty(reader.dataXName)) {
            throw new IllegalStateException("reader.dataXName can not be empty");
        }

        IDataxProcessor processor = DataxProcessor.load(null, reader.dataXName);
        TableAliasMapper tabAlias = processor.getTabAlias(null);
        Optional<TableAlias> findMapper = tabAlias.findFirst();
        if (findMapper.isPresent()) {
            IDataxProcessor.TableMap tabMapper = (IDataxProcessor.TableMap) findMapper.get();
            return Collections.singletonList(
                    new IDataxProcessor.TableMap(Optional.of(DataXDFSReaderContext.FTP_TASK)
                            , tabMapper.getSourceCols()).getSourceTab());
        }

        return dfsReader.getDfsLinker().useTdfsSession((session) -> {

            Set<ITDFSSession.Res> matchRes = this.findAllRes(session);
            for (ITDFSSession.Res res : matchRes) {
                try (InputStream resStream = session.getInputStream(res.fullPath)) {

                    FileFormat.FileHeader fileHeader = dfsReader.getFileFormat(Optional.empty()).readHeader(resStream);

                    ParseColsResult parseColsResult = ParseColsResult.parseColsResult(DataXDFSReaderContext.FTP_TASK, fileHeader);
                    if (!parseColsResult.success) {
                        throw new IllegalStateException("parseColsResult must be success");
                    }
                    return Collections.singletonList(parseColsResult.tabMeta);
                }
            }
            throw new IllegalStateException("have not find any matchRes by resMatcher:" + this.toString());
        });
    }

    @Override
    public String toString() {
        return wildcard;
    }

    @Override
    public boolean isRDBMSSupport() {
        return false;
    }

    @Override
    public boolean hasMulitTable(IDFSReader DFSReader) {
        return false;
    }

    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter, IDFSReader dfsReader) {
        IDataxReaderContext readerContext = new DataXDFSReaderContext(dfsReader);
        return IGroupChildTaskIterator.create(readerContext);
    }

    @Override
    public boolean isMatch(ITDFSSession.Res testRes) {
        return FilenameUtils.wildcardMatch(testRes.relevantPath, this.wildcard);
    }


    @TISExtension
    public static class DftDescriptor extends Descriptor<DFSResMatcher> {
        @Override
        public String getDisplayName() {
            return "Wildcard";
        }
    }

}
