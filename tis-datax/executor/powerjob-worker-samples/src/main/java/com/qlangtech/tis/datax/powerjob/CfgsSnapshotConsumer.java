package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/11
 */
public class CfgsSnapshotConsumer implements Consumer<PluginAndCfgsSnapshot> {
    private PluginAndCfgsSnapshot pluginAndCfgsSnapshot;

    @Override
    public void accept(PluginAndCfgsSnapshot pluginAndCfgsSnapshot) {
        this.pluginAndCfgsSnapshot = Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be "
                + "null");
    }

    public void synchronizTpisAndConfs(DefaultExecContext execContext) {

        try {
            PluginAndCfgsSnapshot localSnapshot =
                    PluginAndCfgsSnapshot.getWorkerPluginAndCfgsSnapshot(execContext.getResType(),
                            new TargetResName(execContext.getIndexName()), Collections.emptySet());

            Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be " + "null") //
                    .synchronizTpisAndConfs(localSnapshot, Optional.empty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
