package com.qlangtech.tis.datax.powerjob;

import com.google.common.collect.Sets;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/11
 */
public class CfgsSnapshotConsumer implements Consumer<PluginAndCfgsSnapshot> {
    private PluginAndCfgsSnapshot pluginAndCfgsSnapshot;
    private static final Logger logger = LoggerFactory.getLogger(CfgsSnapshotConsumer.class);
    private static final Set<Integer> processTaskIds = Sets.newConcurrentHashSet();

    @Override
    public void accept(PluginAndCfgsSnapshot pluginAndCfgsSnapshot) {
        this.pluginAndCfgsSnapshot = Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be "
                + "null");
    }

    public void synchronizTpisAndConfs(DefaultExecContext execContext) {
        try {

            if (processTaskIds.add(execContext.getTaskId())) {
                logger.info("taskId:{},resName:{} execute plugin and config synchronize to local"
                        , execContext.getTaskId(), execContext.getIndexName());
                PluginAndCfgsSnapshot localSnapshot =
                        PluginAndCfgsSnapshot.getWorkerPluginAndCfgsSnapshot(execContext.getResType(),
                                new TargetResName(execContext.getIndexName()), Collections.emptySet());

                Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be " + "null") //
                        .synchronizTpisAndConfs(localSnapshot, Optional.empty());
            } else {
                logger.info("taskId:{},resName:{} SKIP plugin and config synchronize to local"
                        , execContext.getTaskId(), execContext.getIndexName());
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
