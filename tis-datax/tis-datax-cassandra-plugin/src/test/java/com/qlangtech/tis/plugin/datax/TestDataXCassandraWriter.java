/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.ds.cassandra.CassandraDatasourceFactory;
import com.qlangtech.tis.plugin.ds.cassandra.TestCassandraDatasourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXCassandraWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXCassandraWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXCassandraWriter.class);
        assertTrue(extraProps.isPresent());
    }


    public void testDescriptorsJSONGenerate() {
        DataXCassandraWriter writer = new DataXCassandraWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());

        JsonUtil.assertJSONEqual(DataXCassandraWriter.class, "cassandra-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
    }

    public void testTemplateGenerate() throws Exception {
        CassandraDatasourceFactory dsFactory = TestCassandraDatasourceFactory.getDS();
        DataXCassandraWriter writer = new DataXCassandraWriter() {
            public CassandraDatasourceFactory getDataSourceFactory() {
                return dsFactory;
            }
            @Override
            public Class<?> getOwnerClass() {
                return DataXCassandraWriter.class;
            }
        };
        writer.template = DataXCassandraWriter.getDftTemplate();
        writer.batchSize = 22;
        writer.consistancyLevel ="ALL";
        writer.connectionsPerHost = 99;
        writer.maxPendingPerConnection = 33;

        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setFrom("application");
        tableMap.setTo("application");
        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();
        ISelectedTab.ColMeta colMeta = null;
        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("user_id");
        colMeta.setType(ISelectedTab.DataXReaderColType.Long);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("user_name");
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("bron_date");
        colMeta.setType(ISelectedTab.DataXReaderColType.Date);
        sourceCols.add(colMeta);

        tableMap.setSourceCols(sourceCols);

        WriterTemplate.valiateCfgGenerate("cassandra-datax-writer-assert.json", writer, tableMap);
    }
}
