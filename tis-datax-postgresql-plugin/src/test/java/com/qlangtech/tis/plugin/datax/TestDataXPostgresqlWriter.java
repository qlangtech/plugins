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
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXPostgresqlWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXPostgresqlWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXPostgresqlWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXPostgresqlWriter esWriter = new DataXPostgresqlWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(esWriter.getDescriptor());

        JsonUtil.assertJSONEqual(DataXPostgresqlReader.class
                , "postgres-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
    }

    public void testTemplateGenerate() throws Exception {

        DataXPostgresqlWriter dataXWriter = new DataXPostgresqlWriter() {

            @Override
            public Class<?> getOwnerClass() {
                return DataXPostgresqlWriter.class;
            }
        };
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();

        WriterTemplate.valiateCfgGenerate("postgres-datax-writer-assert.json", dataXWriter, tableMap);
    }
}
