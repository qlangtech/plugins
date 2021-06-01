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
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.plugin.test.BasicTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-03 23:15
 **/
public class TestDefaultDataxProcessor extends BasicTest {

    public void testSaveProcess() {
        final String appName = "test" + RandomStringUtils.randomAlphanumeric(2);

        try {
            DefaultDataxProcessor dataxProcessor = new DefaultDataxProcessor();
            IDataxProcessor.TableAlias tabAlias = new IDataxProcessor.TableAlias();
            tabAlias.setFrom("customer_order_relation");
            tabAlias.setTo("customer_order_relation1");
            List<IDataxProcessor.TableAlias> tableMaps = Lists.newArrayList(tabAlias);
            dataxProcessor.setTableMaps(tableMaps);

            dataxProcessor.globalCfg = "datax-global-config";
            dataxProcessor.dptId = "356";
            dataxProcessor.recept = "小明";

            IAppSource.save(null, appName, dataxProcessor);

            DefaultDataxProcessor loadDataxProcessor = IAppSource.load(appName);
            assertNotNull("loadDataxProcessor can not be null", loadDataxProcessor);
            assertEquals(dataxProcessor.globalCfg, loadDataxProcessor.globalCfg);
            assertEquals(dataxProcessor.dptId, loadDataxProcessor.dptId);
            assertEquals(dataxProcessor.recept, loadDataxProcessor.recept);

            Map<String, IDataxProcessor.TableAlias> tabAlias1 = loadDataxProcessor.getTabAlias();
            assertEquals(1, tabAlias1.size());
            for (Map.Entry<String, IDataxProcessor.TableAlias> entry : tabAlias1.entrySet()) {
                assertEquals(tabAlias.getFrom(), entry.getKey());

                assertEquals(tabAlias.getFrom(), entry.getValue().getFrom());
                assertEquals(tabAlias.getTo(), entry.getValue().getTo());
            }
        } finally {
            try {
                DataxReader.AppKey appKey = new DataxReader.AppKey(null, appName, IAppSource.class);
                XmlFile storeFile = appKey.getSotreFile();
                FileUtils.forceDelete(storeFile.getFile().getParentFile());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
