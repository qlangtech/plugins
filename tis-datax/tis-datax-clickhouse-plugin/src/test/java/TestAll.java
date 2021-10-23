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
import com.qlangtech.tis.plugin.datax.TestDataXClickhouseWriter;
import com.qlangtech.tis.plugin.ds.clickhouse.TestClickHouseDataSourceFactory;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;
/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-09 09:22
 **/
public class TestAll extends TestCase {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestDataXClickhouseWriter.class);
        suite.addTestSuite(TestClickHouseDataSourceFactory.class);
        return suite;
    }
}