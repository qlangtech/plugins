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

package com.qlangtech.tis.hive.reader;


import com.google.common.collect.Lists;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.AbstractDFSReader;
import com.qlangtech.tis.plugin.datax.DataXDFSReaderWithMeta;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.TableColsMeta;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.IExclusiveTDFSType;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 15:42
 * @see com.alibaba.datax.plugin.reader.hive.HiveReader
 **/
public class DataXHiveReader extends AbstractDFSReader {
    
    @FormField(ordinal = 2, validate = {Validator.require})
    public PartitionFilter ptFilter;

    public DataXHiveReader() {
        this.resMatcher = new HiveDFSResMatcher();
    }

    @Override
    public HiveDFSLinker getDfsLinker() {
        return (HiveDFSLinker) super.getDfsLinker();
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHiveReader.class, "DataXHiveReader-tpl.json");
    }

    private transient int preSelectedTabsHash;

    @Override
    public List<ISelectedTab> getSelectedTabs() {
        //BasicDataXRdbmsReader
        Objects.requireNonNull(this.selectedTabs, "selectedTabs can not be null");
        if (this.preSelectedTabsHash == selectedTabs.hashCode()) {
            return selectedTabs;
        }

        try (TableColsMeta colMeta = this.getDfsLinker().getTabsMeta()) {
            selectedTabs = this.selectedTabs.stream().map((tab) -> {
                ColumnMetaData.fillSelectedTabMeta(tab, (t) -> {
                    return colMeta.get(t.getName());
                });
                return tab;
            }).collect(Collectors.toList());
            this.preSelectedTabsHash = selectedTabs.hashCode();
            return selectedTabs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean hasMulitTable() {
        return CollectionUtils.isNotEmpty(this.selectedTabs);
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) throws TableNotFoundException {
        // return super.getTableMetadata(conn, inSink, table);
        Hiveserver2DataSourceFactory dsFactory = this.getDfsLinker().getDataSourceFactory();
        return dsFactory.getTableMetadata(false, table);
    }


    @Override
    public List<DataXDFSReaderWithMeta.TargetResMeta> getSelectedEntities() {

        List<DataXDFSReaderWithMeta.TargetResMeta> result = Lists.newArrayList();
        DataXDFSReaderWithMeta.TargetResMeta resMeta = null;

        Hiveserver2DataSourceFactory dsFactory = this.getDfsLinker().getDataSourceFactory();
        IHiveMetaStore msClient = dsFactory.createMetaStoreClient();
        List<HiveTable> tabs = msClient.getTables(dsFactory.dbName);
        for (HiveTable tab : tabs) {
            resMeta = new DataXDFSReaderWithMeta.TargetResMeta(tab.getTableName(), (session) -> {
                return dsFactory.getTableMetadata(false, EntityName.parse(tab.getTableName()));
            });
            result.add(resMeta);
        }

        return result;
    }

//    public static PartitionFilter getPtDftVal() {
//        DefaultPartitionFilter dftPartition = new DefaultPartitionFilter();
//        dftPartition.ptFilter = IDumpTable.PARTITION_PT + " = " + HiveTable.KEY_PT_LATEST;
//        return dftPartition;
//    }

    public static List<? extends Descriptor> filter(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            throw new IllegalArgumentException("param descs can not be null");
        }
        return descs.stream().filter((d) -> {
            return (d instanceof IExclusiveTDFSType)
                    && (((IExclusiveTDFSType) d).getTDFSType() == IEndTypeGetter.EndType.HiveMetaStore); //HiveDFSLinker.NAME_DESC.equals(((Descriptor) d).getDisplayName());
        }).collect(Collectors.toList());
    }

    @Override
    public FileFormat getFileFormat(Optional<String> entityName) {
        return this.getDfsLinker()
                .getInputFileFormat(entityName.orElseThrow(() -> new IllegalArgumentException("param entityName can not be null")));
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor implements DataxWriter.IRewriteSuFormProperties {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isRdbms() {
            return true;
        }

        @Override
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.HiveMetaStore;
        }

        @Override
        public <TAB extends SelectedTab> Descriptor<TAB> getRewriterSelectTabDescriptor() {
            return null;
        }

//        @Override
//        public SuFormProperties overwriteSubPluginFormPropertyTypes(SuFormProperties subformProps) throws Exception {
//            return null;
//        }

        @Override
        public String getDisplayName() {
            return this.getEndType().name();
        }
    }
}
