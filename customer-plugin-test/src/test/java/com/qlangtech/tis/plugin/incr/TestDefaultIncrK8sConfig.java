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

package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.RcDeployment;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.plugin.BaiscPluginTest;
import com.qlangtech.tis.plugin.PluginStore;

/**
 * 跑这个单元测试需要事先部署k8s集群
 *
 * @author: baisui 百岁
 * @create: 2020-08-11 11:05
 **/
public class TestDefaultIncrK8sConfig extends BaiscPluginTest {

    private static final String s4totalpay = "search4totalpay";
    private IncrStreamFactory incrFactory;

    @Override
    public void setUp() throws Exception {
        //super.setUp();
        PluginStore<IncrStreamFactory> s4totalpayIncr = TIS.getPluginStore(s4totalpay, IncrStreamFactory.class);
        incrFactory = s4totalpayIncr.getPlugin();
        assertNotNull(incrFactory);
    }

    public void testCreateIncrDeployment() throws Exception {

        IRCController incr = incrFactory.getIncrSync();
        assertNotNull(incr);
        assertFalse(s4totalpay + " shall have not deploy incr instance in k8s", incr.getRCDeployment(s4totalpay) != null);

        RcDeployment incrSpec = new RcDeployment();
        incrSpec.setCpuLimit(Specification.parse("1"));
        incrSpec.setCpuRequest(Specification.parse("500m"));
        incrSpec.setMemoryLimit(Specification.parse("1G"));
        incrSpec.setMemoryRequest(Specification.parse("500M"));
        incrSpec.setReplicaCount(1);

        long timestamp = 20190820171040l;

        try {
            incr.deploy(s4totalpay, incrSpec, timestamp);
        } catch (Exception e) {
            throw e;
        }

    }


    public void testDeleteIncrDeployment() throws Exception {
        try {
            incrFactory.getIncrSync().removeInstance(s4totalpay);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
