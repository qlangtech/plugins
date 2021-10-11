/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.yarn.IYarnConfig;
import com.qlangtech.tis.manage.common.HttpUtils;
import junit.framework.TestCase;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * @author: baisui 百岁
 * @create: 2020-04-22 18:50
 **/
public class TestYarnTableDumpFactory extends TestCase {
    public void testGetManagerResourceAddress() throws Exception {
        HttpUtils.addMockGlobalParametersConfig();
        IYarnConfig yarn1 = ParamsConfig.getItem("yarn1", IYarnConfig.class);

       // YarnConfiguration config = Hadoop020RemoteJobTriggerFactory.getYarnConfig(yarn1);

//        System.out.println(config.get("yarn.resourcemanager.scheduler.class"));
//
//        String hostname = config.get("yarn.resourcemanagerr.hostname");

      //  System.out.println(hostname);
    }
}
