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

import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.common.PluginDesc;
import junit.framework.TestCase;

import java.util.regex.Matcher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 09:37
 **/
public class TestK8SDataXJobWorker extends TestCase {
    static {
        CenterResource.setNotFetchFromCenterRepository();
        HttpUtils.addMockGlobalParametersConfig();
    }

    public void testZKhostPattern() {
        //  K8SDataXJobWorker dataXJobWorker = new K8SDataXJobWorker();

        Matcher matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200:2181/tis/cloude");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200:2181");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200:2181/tis/cloude-1_bb");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zkhost_pattern.matcher("192.168.28.200");
        assertFalse(matcher.matches());

        matcher = K8SDataXJobWorker.zk_path_pattern.matcher("/tis/cloude");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zk_path_pattern.matcher("/0tis");
        assertTrue(matcher.matches());

        matcher = K8SDataXJobWorker.zk_path_pattern.matcher("0tis");
        assertFalse(matcher.matches());

        matcher = K8SDataXJobWorker.zk_path_pattern.matcher("/tis*_");
        assertFalse(matcher.matches());

        matcher = K8SDataXJobWorker.zk_path_pattern.matcher("/tis/");
        assertFalse(matcher.matches());
    }

    public void testDescGenerate() {

        PluginDesc.testDescGenerate(K8SDataXJobWorker.class, "k8s-datax-job-worker-descriptor.json");
    }

//    public void testGetRCDeployment() {
//        PluginStore<DataXJobWorker> jobWorkerStore = TIS.getPluginStore(DataXJobWorker.class);
//        DataXJobWorker dataxJobWorker = jobWorkerStore.getPlugin();
//        assertNotNull(dataxJobWorker);
//
//        RcDeployment rcMeta = dataxJobWorker.getRCDeployment();
//        assertNotNull(rcMeta);
//    }
}
