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

import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataXBasicProcessMeta;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.ftpreader.FtpReader
 **/
@Public
public abstract class AbstractDFSReader extends DataxReader implements DataXBasicProcessMeta.IRDBMSSupport, Supplier<List<ISelectedTab>>, IDFSReader, KeyedPluginStore.IPluginKeyAware {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDFSReader.class);
    public static final String KEY_DFS_LINKER = "dfsLinker";
    public static final String KEY_RES_MATCHER = "resMatcher";

    public String dataXName;


    @FormField(ordinal = 1, validate = {Validator.require})
    public TDFSLinker dfsLinker;

    @FormField(ordinal = 2, validate = {Validator.require})
    public DFSResMatcher resMatcher;


    /**
     * ================================================================================
     * support rdbms start
     * ================================================================================
     */
    @SubForm(desClazz = SelectedTab.class
            , idListGetScript = "return com.qlangtech.tis.plugin.datax.DataXDFSReaderWithMeta.getDFSFiles(filter);", atLeastOne = true)
    public transient List<SelectedTab> selectedTabs;

    @Override
    public List<ISelectedTab> get() {
        return this.selectedTabs != null
                ? Collections.unmodifiableList(this.selectedTabs) : Collections.emptyList();
    }

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) throws TableNotFoundException {
        return this.resMatcher.getTableMetadata(this, table);
    }


    @Override
    public TDFSLinker getDfsLinker() {
        return this.dfsLinker;
    }

    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {
        return this.resMatcher.getSubTasks(filter, this);
    }


    @Override
    public boolean hasMulitTable() {
        return this.resMatcher.hasMulitTable(this);
    }


    @Override
    public List<ISelectedTab> getSelectedTabs() {
        return this.resMatcher.getSelectedTabs(this);
    }

    /**
     * ================================================================================
     * support rdbms END
     * ================================================================================
     */


    @Override
    public boolean isRDBMSSupport() {
        return Objects.requireNonNull(resMatcher, "resMatcher can not be null").isRDBMSSupport();
    }

    @Override
    public final TableInDB getTablesInDB() {
        BaseDataxReaderDescriptor desc = (BaseDataxReaderDescriptor) this.getDescriptor();
        final TableInDB tableInDB = TableInDB.create(new DBIdentity() {
            @Override
            public boolean isEquals(DBIdentity queryDBSourceId) {
                return true;
            }

            @Override
            public String identityValue() {
                return desc.getEndType().getVal();
            }
        });
        return tableInDB;
    }


    @FormField(ordinal = 16, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;


    @Override
    public String getTemplate() {
        return template;
    }


}
