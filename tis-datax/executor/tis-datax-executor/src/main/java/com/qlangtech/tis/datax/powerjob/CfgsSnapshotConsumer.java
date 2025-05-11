package com.qlangtech.tis.datax.powerjob;

import com.google.common.collect.Maps;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.plugin.PluginAndCfgSnapshotLocalCache;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/11
 */
public class CfgsSnapshotConsumer implements Consumer<PluginAndCfgsSnapshot> {
    // TIS-Console中传输过来的资源快找
    private PluginAndCfgsSnapshot pluginAndCfgsSnapshot;
    private static final Logger logger = LoggerFactory.getLogger(CfgsSnapshotConsumer.class);
    private static final ConcurrentMap<Integer, Long> processTaskIds = Maps.newConcurrentMap();
    private static final long processTaskIdsExpirTime = TimeUnit.MINUTES.toMillis(2);
    private boolean successSync = false;

    @Override
    public void accept(PluginAndCfgsSnapshot pluginAndCfgsSnapshot) {
        this.pluginAndCfgsSnapshot = Objects.requireNonNull(pluginAndCfgsSnapshot, "pluginAndCfgsSnapshot can not be "
                + "null");
    }


    public void synchronizTpisAndConfs(AbstractExecContext execContext, PluginAndCfgSnapshotLocalCache snapshotLocalCache) {
        try {
            synchronized (processTaskIds) {
                final long current = System.currentTimeMillis();

                processTaskIds.compute(execContext.getTaskId(), (tskId, oldVal) -> {

                    if (oldVal == null || (current - oldVal) > processTaskIdsExpirTime) {
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
                                /** avoid throwing excpetion below:
                                 * Caused by: java.lang.IllegalStateException: Recursive update
                                 * 	at java.base/java.util.concurrent.ConcurrentHashMap.replaceNode(ConcurrentHashMap.java:1167)
                                 * 	at java.base/java.util.concurrent.ConcurrentHashMap.remove(ConcurrentHashMap.java:1102)
                                 * 	at com.qlangtech.tis.datax.powerjob.CfgsSnapshotConsumer.lambda$synchronizTpisAndConfs$1(CfgsSnapshotConsumer.java:79)
                                 */
                              //   processTaskIds.remove(execContext.getTaskId());
                            }
                        }
                        return current;
                    } else {
                        logger.info("taskId:{},resName:{} SKIP plugin and config synchronize to local"
                                , execContext.getTaskId(), execContext.getIndexName());
                        return oldVal;
                    }


                });

//                if (processTaskIds.add(execContext.getTaskId())) {
//
//                } else {
//
//                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
