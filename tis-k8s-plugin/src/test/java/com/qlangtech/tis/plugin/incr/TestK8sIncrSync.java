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

import com.qlangtech.tis.coredefine.module.action.IIncrSync;
import com.qlangtech.tis.coredefine.module.action.RcDeployment;
import com.qlangtech.tis.manage.common.Config;
import junit.framework.TestCase;

/**
 * @author: baisui 百岁
 * @create: 2020-09-02 15:06
 **/
public class TestK8sIncrSync extends TestCase {


    public void testGetRCDeployment() {
        IIncrSync incrSync = TestDefaultIncrK8sConfig.getIncrSync();
        RcDeployment rcDeployment = incrSync.getRCDeployment(TestDefaultIncrK8sConfig.totalpay);
        assertNotNull(rcDeployment);
    }


    public void testDeleteDeployment() throws Exception {
        IIncrSync incrSync = TestDefaultIncrK8sConfig.getIncrSync();

        incrSync.removeInstance(Config.S4TOTALPAY);
    }

    public void testLaunchIncrProcess() throws Exception {
        IIncrSync incrSync = TestDefaultIncrK8sConfig.getIncrSync();

        incrSync.relaunch(Config.S4TOTALPAY);

    }
}
