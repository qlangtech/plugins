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

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-13 22:49
 * @see WildcardDFSResMatcher
 * @see MetaAwareDFSResMatcher
 **/
public abstract class BasicDFSResMatcher extends DFSResMatcher {

    @Override
    public List<ColumnMetaData> getTableMetadata(IDFSReader dfsReader, TableMap tableMapper) throws TableNotFoundException {
        return ColumnMetaData.convert(tableMapper.getSourceCols());
    }

    protected static SourceColsMeta getSourceColsMeta(
            IDataxProcessor processor, String tabName, List<ColumnMetaData> colsMeta) {
        IDataxReader reader = processor.getReader(null);
        List<ISelectedTab> selectedTabs = reader.getSelectedTabs();


        Optional<ISelectedTab> tab = Objects.requireNonNull(selectedTabs, "selectedTabs can not be null")
                .stream().filter((t) -> StringUtils.equals(tabName, t.getName())).findFirst();

        ISelectedTab ttab = tab.orElseThrow(() -> new IllegalStateException("can not find tab:" + tabName + " in select tables:"
                + selectedTabs.stream().map((t) -> t.getName()).collect(Collectors.joining(","))));

        final Set<String> selectedCols
                = ttab.getCols().stream().map((c) -> c.getName()).collect(Collectors.toSet());


        List<CMeta> result = Lists.newArrayList();
        CMeta cm = null;

        for (ColumnMetaData col : colsMeta) {
            cm = new CMeta();
            cm.setPk(col.isPk());
            cm.setType(col.getType());
            cm.setName(col.getName());
            cm.setNullable(col.isNullable());
            result.add(cm);
        }
        return new SourceColsMeta(result, (col) -> selectedCols.contains(col));
    }
}
