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

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IFSWriter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-01 10:05
 **/
public abstract class BasicFSWriter extends DataxWriter implements KeyedPluginStore.IPluginKeyAware, IFSWriter {

    protected static final String KEY_FIELD_NAME_HIVE_CONN = "hiveConn";

    @FormField(ordinal = 5, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String fsName;

    @FormField(ordinal = 10, validate = {Validator.require})
    public FSFormat fileType;

    @FormField(ordinal = 15, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public String writeMode;

    @FormField(ordinal = 25, type = FormFieldType.ENUM, validate = {})
    public String compress;
    @FormField(ordinal = 30, type = FormFieldType.ENUM, advance = true, validate = {})
    public String encoding;

    public String dataXName;

    public static IFSWriter getWriterPlugin(String dataxName) {
        return getWriterPlugin(dataxName, StoreResourceType.DataApp);
    }

    public static IFSWriter getWriterPlugin(String dataxName, StoreResourceType resType) {
        DataxWriter dataxWriter = load(null, resType, dataxName, true);
        if (!(dataxWriter instanceof IFSWriter)) {
            throw new BasicHdfsWriterJob.JobPropInitializeException("datax Writer must be type of 'BasicFSWriter',but now is:" + dataxWriter.getClass());
        }
        return (IFSWriter) dataxWriter;
    }

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    private FileSystemFactory fileSystem;

    public FileSystemFactory getFs() {
        if (fileSystem == null) {
            if (StringUtils.isEmpty(this.fsName)) {
                throw new IllegalStateException("prop field:" + this.fsName + " can not be empty");
            }
            this.fileSystem = FileSystemFactory.getFsFactory(fsName);
        }
        Objects.requireNonNull(this.fileSystem, "fileSystem has not be initialized");
        return fileSystem;
    }

    @Override
    public final void startScanDependency() {
        this.getFsFactory();
        this.startScanFSWriterDependency();
    }

    protected abstract void startScanFSWriterDependency();

    @Override
    public FileSystemFactory getFsFactory() {
        return this.getFs();
    }

    @Override
    public final IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        if (StringUtils.isBlank(this.dataXName)) {
            throw new IllegalStateException("param 'dataXName' can not be null");
        }
        FSDataXContext dataXContext = getDataXContext(tableMap.get(), transformerRules);

        return dataXContext;
        //  throw new RuntimeException("dbName:" + dbName + " relevant DS is empty");
    }

    protected abstract FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap, Optional<RecordTransformerRules> transformerRules);

    public  class FSDataXContext implements IDataxContext {

        protected final IDataxProcessor.TableMap tabMap;
        private final String dataxName;
        private final List<IColMetaGetter> cols;

        public FSDataXContext(IDataxProcessor.TableMap tabMap, String dataxName, Optional<RecordTransformerRules> transformerRules) {
            Objects.requireNonNull(tabMap, "param tabMap can not be null");
            this.tabMap = tabMap;
            this.dataxName = dataxName;
            this.cols = tabMap.appendTransformerRuleCols(transformerRules);
            // IDataxProcessor.TabCols.create(dsFactory, tabMap, transformerRules);
        }

        public String getDataXName() {
            return this.dataxName;
        }

        public String getTableName() {
            String tabName = this.tabMap.getTo();
            if (StringUtils.isBlank(tabName)) {
                throw new IllegalStateException("tabName of tabMap can not be null ,tabMap:" + tabMap);
            }
            return tabName;
        }

        public final List<IColMetaGetter> getCols() {
            return this.cols;
        }


        public String getFileType() {
            return fileType.getDescriptor().getDisplayName();
        }

        public String getWriteMode() {
            return writeMode;
        }

        public String getFieldDelimiter() {
            return String.valueOf(fileType.getFieldDelimiter());
        }

        public String getCompress() {
            return compress;
        }

        public String getEncoding() {
            return encoding;
        }


        public boolean isContainCompress() {
            return StringUtils.isNotEmpty(compress);
        }

        public boolean isContainEncoding() {
            return StringUtils.isNotEmpty(encoding);
        }
    }
}
