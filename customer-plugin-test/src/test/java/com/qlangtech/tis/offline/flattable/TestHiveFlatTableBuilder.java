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

package com.qlangtech.tis.offline.flattable;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.offline.FlatTableBuilder;
import com.qlangtech.tis.plugin.BaiscPluginTest;
import com.qlangtech.tis.plugin.PluginStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: baisui 百岁
 * @create: 2020-04-21 13:39
 **/
public class TestHiveFlatTableBuilder extends BaiscPluginTest {
    private PluginStore<FlatTableBuilder> flatTableBuilderStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flatTableBuilderStore = TIS.getPluginStore(FlatTableBuilder.class);
    }


    public void testCreate() {

        // assertNotNull(TSearcherConfigFetcher.get().getLogFlumeAddress());

        // PluginStore<FlatTableBuilder> store = TIS.getPluginStore(FlatTableBuilder.class);
        FlatTableBuilder flatTableBuilder = flatTableBuilderStore.getPlugin();
        assertNotNull(flatTableBuilder);

        AtomicBoolean success = new AtomicBoolean(false);
        flatTableBuilder.startTask((r) -> {
            Connection con = r.getObj();
            assertNotNull(con);
            Statement stmt = null;
            ResultSet result = null;
            try {
                stmt = con.createStatement();
                result = stmt.executeQuery("desc totalpay_summary");
                while (result.next()) {
                    System.out.println("cols:" + result.getString(1));
                }
                success.set(true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {

                try {
                    result.close();
                } catch (SQLException e) {

                }
                try {
                    stmt.close();
                } catch (SQLException e) {

                }
            }
        });

        assertTrue("must success", success.get());
    }

}
