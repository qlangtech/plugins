package com.qlangtech.tis.dag;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import akka.cluster.sharding.ShardRegion;
import akka.pattern.Patterns;
import com.qlangtech.tis.dag.actor.ClusterManagerActor;
import com.qlangtech.tis.dag.actor.DAGSchedulerActor;
import com.qlangtech.tis.dag.actor.NodeDispatcherActor;
import com.qlangtech.tis.dag.actor.WorkflowInstanceActor;
import com.qlangtech.tis.dag.actor.message.WorkflowInstanceMessageExtractor;
import com.qlangtech.tis.datax.ActorSystemStatus;
import com.qlangtech.tis.plugin.akka.DAORestDelegateFacade;
import com.qlangtech.tis.workflow.dao.IDAGNodeExecutionDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 启动 worker-only 节点：
 *   export AKKA_ROLES="worker"
 *   export AKKA_HOSTNAME="192.168.1.101"
 *   export AKKA_PORT=2552
 *   export AKKA_SEED_NODES="akka://TIS-DAG-System@192.168.1.100:2551"
 * 启动seed节点：
 *   export AKKA_HOSTNAME="192.168.1.100"
 *   export AKKA_SEED_NODES="akka://TIS-DAG-System@192.168.1.100:2551" #可以不设置就是127.0.0.1
 *
 * TIS Actor System 管理器
 * 负责 Actor System 的初始化、配置和生命周期管理
 * <p>
 * 核心职责：
 * 1. 初始化 Actor System
 * 2. 加载 application.conf 配置
 * 3. 自动适配单节点和多节点配置
 * 4. 创建所有核心 Actor
 * 5. 优雅关闭
 * <p>
 * 设计说明：
 * - 统一集群架构：单节点和多节点使用相同的配置
 * - 零配置扩展：通过环境变量 AKKA_SEED_NODES 自动适配
 * - 单例模式：整个 TIS 系统只有一个 Actor System 实例
 * - 生命周期管理：与 TIS 启动/关闭流程集成
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class TISActorSystem {
    /**
     * ServletContext 中的属性名
     */
    public static final String ATTR_TIS_ACTOR_SYSTEM = "tisActorSystem";
    private static final String KEY_AKKA_ROLES = "akka.cluster.roles";

    private static TISActorSystem tisActorSystem;

    /**
     * 可以在 DistributedAKKAJobDataXJobSubmit 中调用
     *
     * @return
     */
    public static TISActorSystem get() {
        return Objects.requireNonNull(tisActorSystem, "actorSystem can not be null");
    }

    private static void set(TISActorSystem akkaActorSystem) {
        tisActorSystem = akkaActorSystem;
    }

    public static void main(String[] args) {
//        TISActorSystem akkaSys = TISActorSystem.get();
//        akkaSys.getWorkflowInstanceRegion().tell(queryMsg, ActorRef.noSender());
    }

    public static TISActorSystem createAndInit(DAORestDelegateFacade workflowDAOFacade) {
//        IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO = workflowDAOFacade.getWorkFlowBuildHistoryDAO();
//        IDAGNodeExecutionDAO dagNodeExecutionDAO = workflowDAOFacade.getDagNodeExecutionDAO();
        if (tisActorSystem != null) {
            throw new IllegalStateException("tisActorSystem has been initialized,can not be init twice");
        }
        // 3. 创建 TISActorSystem 实例
        tisActorSystem = new TISActorSystem(workflowDAOFacade);

        // 4. 初始化 Actor System
        tisActorSystem.initialize();
        set(tisActorSystem);
        return tisActorSystem;
    }



    private static final Logger logger = LoggerFactory.getLogger(TISActorSystem.class);

    /**
     * Actor System 名称
     */
    private static final String ACTOR_SYSTEM_NAME = "TIS-DAG-System";

    /**
     * 环境变量：Seed Nodes
     * 格式：akka://TIS-DAG-System@host1:2551,akka://TIS-DAG-System@host2:2551
     */
    private static final String ENV_AKKA_SEED_NODES = "AKKA_SEED_NODES";

    /**
     * 环境变量：当前节点地址
     * 格式：host:port（例如：192.168.1.100:2551）
     */
    private static final String ENV_AKKA_HOSTNAME = "AKKA_HOSTNAME";
    private static final String ENV_AKKA_PORT = "AKKA_PORT";

    /**
     * 环境变量：节点角色
     * 格式：逗号分隔，例如 "worker" 或 "scheduler,worker"
     */
    private static final String ENV_AKKA_ROLES = "AKKA_ROLES";

    /**
     * Actor System 实例
     */
    private ActorSystem actorSystem;

    /**
     * 核心 Actor 引用
     */
    private ActorRef clusterManagerActor;
    private ActorRef nodeDispatcherActor;
    private ActorRef dagSchedulerActor;

    /**
     * Cluster Sharding Region 引用
     * WorkflowInstance的分片区域，用于路由消息到对应的WorkflowInstanceActor
     */
    private ActorRef workflowInstanceRegion;

    /**
     * DAO 依赖
     */
    private final IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO;
    private final IDAGNodeExecutionDAO dagNodeExecutionDAO;

    private final DAORestDelegateFacade daoFacade;
    /**
     * 是否已初始化
     */
    private volatile boolean initialized = false;

    /**
     * Actor System start time
     */
    private long startTimeMillis;

    public TISActorSystem(DAORestDelegateFacade daoFacade) {
        Objects.requireNonNull(daoFacade, "daoFacade can not be null");
        this.workflowBuildHistoryDAO = Objects.requireNonNull(daoFacade.getWorkFlowBuildHistoryDAO() //
                , "workflowBuildHistoryDAO can not be null");
        this.dagNodeExecutionDAO = Objects.requireNonNull(daoFacade.getDagNodeExecutionDAO() //
                , "dagNodeExecutionDAO can not be null");
        this.daoFacade = daoFacade;
    }

    public DAORestDelegateFacade getDaoFacade() {
        return this.daoFacade;
    }

    /**
     * 初始化 Actor System
     * <p>
     * 实现步骤：
     * 1. 加载 application.conf 配置
     * 2. 读取环境变量 AKKA_SEED_NODES，自动适配单节点/多节点
     * 3. 创建 Actor System
     * 4. 创建所有核心 Actor
     * 5. 集成到 TIS 启动流程
     * <p>
     * 统一集群架构说明：
     * - 单节点模式：不设置 AKKA_SEED_NODES，使用默认配置（127.0.0.1:2551）
     * - 多节点模式：设置 AKKA_SEED_NODES 环境变量，自动加入集群
     * - 零配置扩展：新节点只需设置环境变量即可加入集群
     */
    public void initialize() {
        if (initialized) {
            logger.warn("TIS Actor System already initialized, skipping...");
            return;
        }

        logger.info("Initializing TIS Actor System...");

        try {
            // 1. 加载配置
            Config config = loadConfig();
            logger.info("Configuration loaded successfully");

            // 2. 创建 Actor System
            actorSystem = ActorSystem.create(ACTOR_SYSTEM_NAME, config, TISActorSystem.class.getClassLoader());
            logger.info("Actor System created: {}", actorSystem.name());

            boolean isScheduler = hasRole(config, "scheduler");
            boolean isWorker = hasRole(config, "worker");
            logger.info("Node roles: scheduler={}, worker={}", isScheduler, isWorker);

            // 3. ClusterManagerActor — 所有节点都需要，用于监控集群事件
            clusterManagerActor = actorSystem.actorOf(ClusterManagerActor.props(), "cluster-manager");
            logger.info("ClusterManagerActor created: {}", clusterManagerActor.path());

            if (isScheduler) {
                // 4. scheduler 节点：创建 NodeDispatcherActor
                nodeDispatcherActor = actorSystem.actorOf(NodeDispatcherActor.props(
                        dagNodeExecutionDAO, workflowBuildHistoryDAO), "node-dispatcher");
                logger.info("NodeDispatcherActor created: {}", nodeDispatcherActor.path());

                // 5. 初始化 Cluster Sharding（需要 nodeDispatcherActor）
                initializeClusterSharding();
                logger.info("Cluster Sharding initialized successfully");

                // 6. 创建 DAGSchedulerActor（需要 workflowInstanceRegion）
                createDAGSchedulerActor();
                logger.info("DAGSchedulerActor created successfully");
            } else {
                logger.info("Non-scheduler node: skipping NodeDispatcherActor, ClusterSharding, DAGSchedulerActor creation");
            }

            // worker 节点不需要创建额外的顶级 Actor
            // ClusterRouterPool 会自动在 worker 角色节点上创建 TaskWorkerActor routee

            // 6. 标记为已初始化
            initialized = true;
            startTimeMillis = System.currentTimeMillis();

            logger.info("TIS Actor System initialized successfully");
            logger.info("Actor System address: {}", actorSystem.provider().getDefaultAddress());

        } catch (Exception e) {
            logger.error("Failed to initialize TIS Actor System", e);
            // 清理资源
            if (actorSystem != null) {
                try {
                    actorSystem.terminate();
                } catch (Exception ex) {
                    logger.error("Failed to terminate Actor System during cleanup", ex);
                }
            }
            throw new IllegalStateException("Failed to initialize Actor System", e);
        }
    }

    /**
     * 加载配置
     * 自动适配单节点和多节点配置
     * <p>
     * 配置优先级：
     * 1. 环境变量（AKKA_SEED_NODES、AKKA_HOSTNAME、AKKA_PORT）
     * 2. application.conf 文件
     * 3. reference.conf（Akka 默认配置）
     * <p>
     * 单节点模式：
     * - 不设置 AKKA_SEED_NODES
     * - 使用默认配置：akka.cluster.seed-nodes = ["akka://TIS-DAG-System@127.0.0.1:2551"]
     * <p>
     * 多节点模式：
     * - 设置 AKKA_SEED_NODES="akka://TIS-DAG-System@host1:2551,akka://TIS-DAG-System@host2:2551"
     * - 可选：设置 AKKA_HOSTNAME 和 AKKA_PORT 指定当前节点地址
     *
     * @return Akka 配置
     */
    private Config loadConfig() {
        logger.info("Loading Akka configuration...");

        // 1. 加载 application.conf
        Config config = ConfigFactory.load(TISActorSystem.class.getClassLoader());

        // 2. 读取环境变量 AKKA_SEED_NODES
        String seedNodes = System.getenv(ENV_AKKA_SEED_NODES);
        if (seedNodes != null && !seedNodes.trim().isEmpty()) {
            // 多节点模式：使用环境变量中的 seed nodes
            logger.info("Running in multi-node mode");
            logger.info("Seed nodes from environment: {}", seedNodes);

            // 解析 seed nodes（逗号分隔）
            String[] nodes = seedNodes.split(",");
            config = config.withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromIterable(Arrays.asList(nodes)));

            logger.info("Configured {} seed nodes", nodes.length);
        } else {
            // 单节点模式：使用默认配置
            logger.info("Running in single-node mode (no AKKA_SEED_NODES set)");
            logger.info("Using default seed node: akka://{}@127.0.0.1:2551", ACTOR_SYSTEM_NAME);
        }

        // 3. 读取环境变量 AKKA_HOSTNAME 和 AKKA_PORT（可选）
        String hostname = System.getenv(ENV_AKKA_HOSTNAME);
        String port = System.getenv(ENV_AKKA_PORT);

        if (hostname != null && !hostname.trim().isEmpty()) {
            logger.info("Overriding hostname from environment: {}", hostname);
            config = config.withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(hostname));
        }

        if (port != null && !port.trim().isEmpty()) {
            logger.info("Overriding port from environment: {}", port);
            config = config.withValue("akka.remote.artery.canonical.port",
                    ConfigValueFactory.fromAnyRef(Integer.parseInt(port)));
        }

        // 4. 读取 AKKA_ROLES 环境变量（可选）
        String roles = System.getenv(ENV_AKKA_ROLES);
        if (roles != null && !roles.trim().isEmpty()) {
            logger.info("Overriding roles from environment: {}", roles);
            String[] roleArray = roles.split(",");
            config = config.withValue(KEY_AKKA_ROLES,
                    ConfigValueFactory.fromIterable(Arrays.asList(roleArray)));
        }

        // 5. 记录最终配置
        logConfiguration(config);

        return config;
    }

    /**
     * 初始化 Cluster Sharding
     * 为WorkflowInstanceActor配置分片，实现分布式部署
     * <p>
     * Cluster Sharding 优势：
     * 1. 自动路由：基于workflowInstanceId自动路由消息到正确的Actor
     * 2. 负载均衡：Actor实例均匀分布在集群节点上
     * 3. 故障恢复：节点宕机后，Actor自动在其他节点重启
     * 4. 动态扩缩容：新节点加入后自动接管部分Actor
     * <p>
     * 分片策略：
     * - 分片数：30（建议为节点数的10倍）
     * - 路由键：workflowInstanceId
     * - 分片算法：Hash取模
     */
    private void initializeClusterSharding() {
        logger.info("Initializing Cluster Sharding for WorkflowInstance...");

        try {
            // 获取ClusterSharding扩展
            ClusterSharding sharding = ClusterSharding.get(actorSystem);

            // 创建WorkflowInstance的Sharding Region
            // 参数说明：
            // 1. typeName: "WorkflowInstance" - 实体类型名称
            // 2. entityProps: WorkflowInstanceActor的Props
            // 3. settings: Sharding配置
            // 4. messageExtractor: 消息提取器，用于路由
            workflowInstanceRegion = sharding.start("WorkflowInstance", //
                    WorkflowInstanceActor.props( //
                            Objects.requireNonNull(nodeDispatcherActor, "nodeDispatcherActor can not be null"),
                            workflowBuildHistoryDAO)//
                    , ClusterShardingSettings.create(actorSystem) //
                    , new WorkflowInstanceMessageExtractor());

            logger.info("Cluster Sharding initialized: WorkflowInstance region created at {}",
                    workflowInstanceRegion.path());

            // 将 workflowInstanceRegion 注入到 NodeDispatcherActor，解决循环依赖
            // NodeDispatcherActor 需要通过 Region 转发 NodeCompleted 消息，
            // 而不是直接使用 entity ActorRef（可能因 shard rebalancing/passivation 失效）
            nodeDispatcherActor.tell(
                    new NodeDispatcherActor.SetWorkflowInstanceRegion(workflowInstanceRegion),
                    ActorRef.noSender());

        } catch (Exception e) {
            logger.error("Failed to initialize Cluster Sharding", e);
            throw new IllegalStateException("Failed to initialize Cluster Sharding", e);
        }
    }

    /**
     * Create DAGSchedulerActor for cron-based scheduling.
     * Must be called after initializeClusterSharding() since it needs workflowInstanceRegion.
     */
    private void createDAGSchedulerActor() {
        logger.info("Creating DAGSchedulerActor...");
        try {
            // 后续优化可以使用 ClusterSingletonManager包装，保证集群中使用一个dagSchedulerActor活着
            dagSchedulerActor = actorSystem.actorOf(
                    DAGSchedulerActor.props(workflowInstanceRegion, workflowBuildHistoryDAO),
                    "dag-scheduler");
            logger.info("DAGSchedulerActor created: {}", dagSchedulerActor.path());
        } catch (Exception e) {
            logger.error("Failed to create DAGSchedulerActor", e);
            throw new IllegalStateException("Failed to create DAGSchedulerActor", e);
        }
    }

    /**
     * Collect Actor System status for monitoring dashboard
     *
     * @return ActorSystemStatus with basic system info
     */
    public ActorSystemStatus collectStatus() {
        ActorSystemStatus status = new ActorSystemStatus();
        status.setSystemName(ACTOR_SYSTEM_NAME);
        status.setInitialized(initialized);
        status.setRunning(isRunning());
        status.setStartTime(startTimeMillis);

        if (actorSystem != null) {
            akka.actor.Address defaultAddress = actorSystem.provider().getDefaultAddress();
            status.setAddress(defaultAddress.toString());
            if (defaultAddress.host().isDefined()) {
                status.setHostname(defaultAddress.host().get());
            }
            if (defaultAddress.port().isDefined()) {
                status.setPort((Integer) defaultAddress.port().get());
            }
            status.setUptime(actorSystem.uptime() * 1000L);

            // Collect actor counts
            java.util.Map<String, Integer> actorCounts = new java.util.HashMap<>();
            actorCounts.put("ClusterManagerActor", clusterManagerActor != null ? 1 : 0);
            actorCounts.put("NodeDispatcherActor", nodeDispatcherActor != null ? 1 : 0);
            actorCounts.put("WorkflowInstanceRegion", workflowInstanceRegion != null ? 1 : 0);
            actorCounts.put("DAGSchedulerActor", dagSchedulerActor != null ? 1 : 0);
            status.setActorCounts(actorCounts);
        }

        return status;
    }

    /**
     * 优雅关闭 Actor System
     * <p>
     * 关闭步骤：
     * 1. 停止接收新的工作流请求
     * 2. 等待正在执行的任务完成（可选）
     * 3. 停止所有 Actor
     * 4. 终止 Actor System
     * 5. 等待终止完成
     * <p>
     * 超时设置：
     * - 默认等待 30 秒
     * - 超时后强制终止
     */
    public void shutdown() {
        if (!initialized) {
            logger.warn("TIS Actor System not initialized, skipping shutdown...");
            return;
        }

        logger.info("Shutting down TIS Actor System...");

        if (actorSystem != null) {
            try {
                // 1. 终止 Actor System
                // 说明：这会向所有 Actor 发送 PoisonPill 消息
                actorSystem.terminate();
                logger.info("Actor System termination initiated");

                // 2. 等待终止完成
                // 说明：最多等待 30 秒
                Await.result(actorSystem.whenTerminated(), Duration.create(30, TimeUnit.SECONDS));
                logger.info("TIS Actor System shutdown completed");

                // 3. 标记为未初始化
                initialized = false;

            } catch (Exception e) {
                logger.error("Failed to shutdown TIS Actor System gracefully", e);
                // 即使出错也标记为未初始化
                initialized = false;
            }
        }
    }

    /**
     * 检查配置中是否包含指定的集群角色
     *
     * @param config Akka config
     * @param role   role name to check
     * @return true if the role is present
     */
    private static boolean hasRole(Config config, String role) {
        if (config.hasPath(KEY_AKKA_ROLES)) {
            return config.getStringList(KEY_AKKA_ROLES).contains(role);
        }
        return false;
    }

    /**
     * 记录配置信息
     * <p>
     * 用于调试和问题排查
     *
     * @param config Akka 配置
     */
    private void logConfiguration(Config config) {
        logger.info("=== Akka Configuration ===");

        try {
            // 记录关键配置项
            if (config.hasPath("akka.remote.artery.canonical.hostname")) {
                logger.info("Hostname: {}", config.getString("akka.remote.artery.canonical.hostname"));
            }

            if (config.hasPath("akka.remote.artery.canonical.port")) {
                logger.info("Port: {}", config.getInt("akka.remote.artery.canonical.port"));
            }

            if (config.hasPath("akka.cluster.seed-nodes")) {
                logger.info("Seed nodes: {}", config.getStringList("akka.cluster.seed-nodes"));
            }

            if (config.hasPath(KEY_AKKA_ROLES)) {
                logger.info("Roles: {}", config.getStringList(KEY_AKKA_ROLES));
            }

            if (config.hasPath("akka.cluster.split-brain-resolver.active-strategy")) {
                logger.info("Split brain resolver: {}", config.getString("akka.cluster.split-brain-resolver" +
                        ".active-strategy"));
            }

        } catch (Exception e) {
            logger.warn("Failed to log configuration details", e);
        }

        logger.info("==========================");
    }

    /**
     * 检查 Actor System 是否已初始化
     *
     * @return true 如果已初始化
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * 检查 Actor System 是否正在运行
     *
     * @return true 如果正在运行
     */
    public boolean isRunning() {
        return initialized && actorSystem != null && !actorSystem.whenTerminated().isCompleted();
    }

    // Getters

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    public ActorRef getClusterManagerActor() {
        return clusterManagerActor;
    }

    public ActorRef getNodeDispatcherActor() {
        return nodeDispatcherActor;
    }

    /**
     * 获取 WorkflowInstance Sharding Region
     * 用于直接向WorkflowInstanceActor发送消息
     *
     * @return WorkflowInstance Sharding Region引用
     */
    public ActorRef getWorkflowInstanceRegion() {
        return workflowInstanceRegion;
    }

    /**
     * 获取 DAGSchedulerActor 引用
     * 用于发送 RegisterSchedule/UnregisterSchedule 等消息
     *
     * @return DAGSchedulerActor 引用
     */
    public ActorRef getDAGSchedulerActor() {
        return dagSchedulerActor;
    }

    /**
     * 检查指定 taskId 的 WorkflowInstanceActor 是否正在运行
     * 通过查询 Cluster Sharding Region 状态实现，不会触发 Actor 自动创建
     *
     * @param taskId workflow instance task id
     * @return true if the actor is active in the shard region
     */
    public boolean isWorkflowInstanceActive(Integer taskId) {
        try {
            Duration timeout = Duration.create(5, TimeUnit.SECONDS);
            Future<Object> future = Patterns.ask(
                    workflowInstanceRegion,
                    ShardRegion.getShardRegionStateInstance(),
                    timeout.toMillis());

            Object result = Await.result(future, timeout);
            ShardRegion.CurrentShardRegionState state = (ShardRegion.CurrentShardRegionState) result;

            String entityId = String.valueOf(taskId);
            for (ShardRegion.ShardState shardState : JavaConverters.setAsJavaSetConverter(state.shards()).asJava()) {
                if (JavaConverters.setAsJavaSetConverter(shardState.entityIds()).asJava().contains(entityId)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            logger.warn("failed to check workflow instance active status, taskId: {}", taskId, e);
            return false;
        }
    }
}
