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

import com.qlangtech.tis.plugin.datax.TestDataXGlobalConfig;
import com.qlangtech.tis.plugin.datax.TestDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.TestDefaultDataxProcessor;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author: baisui 百岁
 * @create: 2021-04-21 11:31
 **/
public class TestAll {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestDataXGlobalConfig.class);
        suite.addTestSuite(TestDataXJobSubmit.class);
        suite.addTestSuite(TestDefaultDataxProcessor.class);
        return suite;
    }

}
