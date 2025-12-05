package com.qlangtech.tis.plugin.ds.kingbase;

import com.qlangtech.tis.extension.TISExtension;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/11/30
 */
public class KingBaseDataSourceFactory extends BasicKingBaseDataSourceFactory {

    @Override
    protected String getDBType() {
        return JDBC_SCHEMA_TYPE_V8;
    }

    @Override
    protected java.sql.Driver createDriver() {
        return new com.kingbase8.Driver();
    }

    @TISExtension
    public static class V8 extends BaiscKingBaseDSDescriptor {
        @Override
        protected String getDataSourceName() {
            return KingBase_NAME + KingBase_Ver8;
        }
    }
}
