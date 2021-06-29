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

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 15:17
 **/
public class TestDataXHiveWriter extends BasicTest {

    public void testGetDftTemplate() {
        String dftTemplate = DataXHiveWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXHiveWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXHiveWriter writer = new DataXHiveWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());

        JSONObject desc = descJson.getDescriptorsJSON();
        System.out.println(JsonUtil.toString(desc));

        JsonUtil.assertJSONEqual(TestDataXHiveWriter.class, "desc-json/datax-writer-hive.json", desc, (m, e, a) -> {
            assertEquals(m, e, a);
        });
    }

    String mysql2hiveDataXName = "mysql2hive";

    public void testConfigGenerate() throws Exception {


        DataXHiveWriter hiveWriter = new DataXHiveWriter();
        hiveWriter.dataXName = mysql2hiveDataXName;
        hiveWriter.fsName = "hdfs1";
        hiveWriter.fileType = "text";
        hiveWriter.writeMode = "nonConflict";
        hiveWriter.fieldDelimiter = "\t";
        hiveWriter.compress = "gzip";
        hiveWriter.encoding = "utf-8";
        hiveWriter.template = DataXHiveWriter.getDftTemplate();
        hiveWriter.partitionRetainNum = 2;
        hiveWriter.partitionFormat = "yyyyMMdd";


        IDataxProcessor.TableMap tableMap = TestDataXHdfsWriter.createCustomer_order_relationTableMap();


        WriterTemplate.valiateCfgGenerate("hive-datax-writer-assert.json", hiveWriter, tableMap);


        hiveWriter.compress = null;
        hiveWriter.encoding = null;

        WriterTemplate.valiateCfgGenerate("hive-datax-writer-assert-without-option-val.json", hiveWriter, tableMap);

    }


    public void testDataDump() throws Exception {

        final DataxWriter dataxWriter = DataxWriter.load(null, mysql2hiveDataXName);

        WriterTemplate.realExecuteDump("hive-datax-writer-assert-without-option-val.json", dataxWriter);
    }


}
