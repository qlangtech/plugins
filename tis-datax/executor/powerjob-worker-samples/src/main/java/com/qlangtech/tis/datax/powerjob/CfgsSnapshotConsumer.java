package com.qlangtech.tis.datax.powerjob;

import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.plugin.PluginAndCfgSnapshotLocalCache;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/11
 */
public class CfgsSnapshotConsumer implements Consumer<PluginAndCfgsSnapshot> {
    // TIS-Console中传输过来的资源快找
    private PluginAndCfgsSnapshot pluginAndCfgsSnapshot;
    private static final Logger logger = LoggerFactory.getLogger(CfgsSnapshotConsumer.class);
    private static final Set<Integer> processTaskIds = Sets.newConcurrentHashSet();
    private boolean successSync = false;

    @Override
    public void accept(PluginAndCfgsSnapshot pluginAndCfgsSnapshot) {
        this.pluginAndCfgsSnapshot = Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be "
                + "null");
    }

//    /**
//     * 经过同步之后逻辑之后会返回 CfgsSnapshot
//     *
//     * @return
//     */
//    public PluginAndCfgsSnapshot getCfgsSnapshotWhenSuccessSync() {
//        return successSync ? pluginAndCfgsSnapshot : null;
//    }

    public void synchronizTpisAndConfs(DefaultExecContext execContext, PluginAndCfgSnapshotLocalCache snapshotLocalCache) {
        try {
            if (processTaskIds.add(execContext.getTaskId())) {
                try {
                    logger.info("taskId:{},resName:{} execute plugin and config synchronize to local"
                            , execContext.getTaskId(), execContext.identityValue());
                    TargetResName resName = null;
                    switch (execContext.getResType()) {
                        case DataApp:
                            resName = new TargetResName(execContext.getIndexName());
                            break;
                        case DataFlow:
                            resName = new TargetResName(execContext.getWorkflowName());
                            break;
                        default:
                            throw new IllegalStateException("illegal type:" + execContext.getResType());
                    }
                    PluginAndCfgsSnapshot localSnapshot =
                            PluginAndCfgsSnapshot.getWorkerPluginAndCfgsSnapshot(execContext.getResType(), resName
                                    , Collections.emptySet());

                    snapshotLocalCache.processLocalCache(new TargetResName(execContext.identityValue()), (cacheSnaphsot) -> {
                        try {

                            Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be null") //
                                    .synchronizTpisAndConfs(localSnapshot, cacheSnaphsot);
                            TIS.permitInitialize = true;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return pluginAndCfgsSnapshot;
                    });

                    successSync = true;

                } finally {
                    if (!successSync) {
                        processTaskIds.remove(execContext.getTaskId());
                    }
                }
            } else {
                logger.info("taskId:{},resName:{} SKIP plugin and config synchronize to local"
                        , execContext.getTaskId(), execContext.getIndexName());
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
