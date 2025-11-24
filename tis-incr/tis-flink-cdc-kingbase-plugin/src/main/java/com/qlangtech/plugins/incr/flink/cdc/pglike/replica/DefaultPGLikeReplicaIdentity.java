package com.qlangtech.plugins.incr.flink.cdc.pglike.replica;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.cdc.pglike.PGLikeReplicaIdentity;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.util.List;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/11/23
 */
public class DefaultPGLikeReplicaIdentity extends PGLikeReplicaIdentity {
    @Override
    public boolean isShallContainBeforeVals() {
        return false;
    }

    @Override
    public boolean validateSelectedTabs(IControlMsgHandler msgHandler
            , Context context, IDataSourceFactoryGetter dataSourceGetter, List<ISelectedTab> selectedTabs) {
        return true;
    }


    @TISExtension
    public static final class DefaultDesc extends Descriptor<PGLikeReplicaIdentity> {
        @Override
        public String getDisplayName() {
            return DEFAULT;
        }
    }

}
