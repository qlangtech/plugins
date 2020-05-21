package com.qlangtech.tis.offline.flattable;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.common.utils.TSearcherConfigFetcher;
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


    public void testCreate() {

       // assertNotNull(TSearcherConfigFetcher.get().getLogFlumeAddress());

        PluginStore<FlatTableBuilder> store = TIS.getPluginStore(FlatTableBuilder.class);
        FlatTableBuilder flatTableBuilder = store.getPlugin();
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
