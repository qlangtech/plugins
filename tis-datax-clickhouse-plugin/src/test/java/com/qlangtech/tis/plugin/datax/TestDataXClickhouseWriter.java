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

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import org.apache.curator.shaded.com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXClickhouseWriter extends com.qlangtech.tis.plugin.test.BasicTest {
    public void testGetDftTemplate() {
        String dftTemplate = DataXClickhouseWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXClickhouseWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testConfigGenerate() throws Exception {
        ClickHouseDataSourceFactory dsFactory = new ClickHouseDataSourceFactory();
        dsFactory.jdbcUrl = "jdbc:clickhouse://192.168.28.200:8123/tis";
        dsFactory.password = "123123";
        dsFactory.username = "root";
        dsFactory.name = "tisdb";
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setFrom("application");
        tableMap.setTo("application");

        ISelectedTab.ColMeta cm = null;
        List<ISelectedTab.ColMeta> cmetas = Lists.newArrayList();
        cm = new ISelectedTab.ColMeta();
        cm.setName("appid");
        cm.setType(ISelectedTab.DataXReaderColType.Long);
        cmetas.add(cm);
        cm = new ISelectedTab.ColMeta();
        cm.setName("projectname");
        cm.setType(ISelectedTab.DataXReaderColType.STRING);
        cmetas.add(cm);
        tableMap.setSourceCols(cmetas);
        DataXClickhouseWriter writer = new DataXClickhouseWriter() {
            @Override
            public Class<?> getOwnerClass() {
                return DataXClickhouseWriter.class;
            }
            @Override
            public ClickHouseDataSourceFactory getDataSourceFactory() {
                // return super.getDataSourceFactory();
                return dsFactory;
            }
        };
        writer.template = DataXClickhouseWriter.getDftTemplate();
        writer.batchByteSize = 3456;
        writer.batchSize = 9527;
        writer.dbName = "tisdb";
        writer.writeMode = "insert";
       // writer.autoCreateTable = true;
        writer.postSql = "drop table ${table}";
        writer.preSql = "drop table ${table}";

        WriterTemplate.valiateCfgGenerate("clickhouse-datax-writer-assert.json", writer, tableMap);
    }

    public void testDescriptorsJSONGenerate() {
        DataXClickhouseWriter writer = new DataXClickhouseWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());
        //System.out.println(descJson.getDescriptorsJSON().toJSONString());

        JsonUtil.assertJSONEqual(DataXClickhouseWriter.class, "clickhouse-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
    }
}
