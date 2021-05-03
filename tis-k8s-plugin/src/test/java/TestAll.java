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

import com.qlangtech.tis.plugin.datax.TestK8SDataXJobWorker;
import com.qlangtech.tis.plugin.incr.TestDefaultIncrK8sConfig;
import com.qlangtech.tis.plugin.incr.TestK8sIncrSync;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/*
 * @create: 2020-01-14 09:15
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestAll extends TestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite();

        suite.addTestSuite(TestDefaultIncrK8sConfig.class);
        suite.addTestSuite(TestK8sIncrSync.class);
        suite.addTestSuite(TestK8SDataXJobWorker.class);


        return suite;
    }


}
