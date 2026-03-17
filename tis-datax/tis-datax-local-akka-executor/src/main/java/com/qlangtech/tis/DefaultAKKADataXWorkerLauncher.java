package com.qlangtech.tis;

import com.qlangtech.tis.dag.TISActorSystem;
import com.qlangtech.tis.datax.DataXJobSubmitParams;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.akka.DAORestDelegateFacade;
import com.qlangtech.tis.plugin.datax.DataXWorkerLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataX Worker 任务执行器
 * <pre>
 *     export AKKA_ROLES="worker"
 *     export AKKA_HOSTNAME="192.168.28.189"
 *     export AKKA_PORT=2552
 *     export AKKA_SEED_NODES="akka://TIS-DAG-System@192.168.28.189:2551"
 * </pre>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/5
 */
@TISExtension
public class DefaultAKKADataXWorkerLauncher extends DataXWorkerLauncher {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAKKADataXWorkerLauncher.class);

    @Override
    public void start() {
        DataXJobSubmitParams submitParams = DataXJobSubmitParams.getDftIfEmpty();
        logger.info("start to launch DataX worker,maxInstancesPerNode:{},maxTotalNrOfInstances:{}", submitParams.maxInstancesPerNode, submitParams.maxTotalNrOfInstances);
        DAORestDelegateFacade akkaClusterDependenceDao = DAORestDelegateFacade.createAKKAClusterDependenceDao();
        // 3. 创建 TISActorSystem 实例
        TISActorSystem tisActorSystem = TISActorSystem.createAndInit(akkaClusterDependenceDao);

        // 4. 初始化 Actor System
        tisActorSystem.initialize();
        logger.info("launch DataX Worker successful");
    }

    public static void main(String[] args) {
        DefaultAKKADataXWorkerLauncher launcher = new DefaultAKKADataXWorkerLauncher();
        launcher.start();
    }
}
