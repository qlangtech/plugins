package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import com.qlangtech.tis.dag.actor.message.QueryClusterStatus;
import com.qlangtech.tis.datax.ActorSystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 集群管理器 Actor
 * 负责集群成员管理和故障恢复
 *
 * 核心职责：
 * 1. 订阅 Akka Cluster 事件
 * 2. 处理节点上线事件（MemberUp）
 * 3. 处理节点下线事件（MemberRemoved）
 * 4. 处理节点不可达事件（UnreachableMember）
 * 5. 集群状态查询
 *
 * 设计说明：
 * - 维护集群成员列表
 * - 监控节点健康状态
 * - 触发故障恢复机制
 * - 支持任务重新平衡（可选）
 * - 与 NodeDispatcherActor 协作进行任务调度
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class ClusterManagerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManagerActor.class);

    /**
     * Akka Cluster 实例
     */
    private final Cluster cluster;

    /**
     * 集群成员列表
     * 记录所有 UP 状态的成员
     */
    private final Set<Member> clusterMembers = new HashSet<>();

    /**
     * 不可达成员列表
     * 记录所有不可达的成员
     */
    private final Set<Member> unreachableMembers = new HashSet<>();

    public ClusterManagerActor() {
        this.cluster = Cluster.get(getContext().getSystem());
    }

    public static Props props() {
        return Props.create(ClusterManagerActor.class, ClusterManagerActor::new);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        // 订阅集群事件
        // initialStateAsEvents() 会立即发送当前集群状态
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                ClusterEvent.MemberEvent.class,
                ClusterEvent.UnreachableMember.class,
                ClusterEvent.ReachableMember.class);

        logger.info("ClusterManagerActor started, subscribed to cluster events");
        logger.info("Current cluster address: {}", cluster.selfAddress());
    }

    @Override
    public void postStop() throws Exception {
        cluster.unsubscribe(getSelf());
        logger.info("ClusterManagerActor stopped, unsubscribed from cluster events");
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.MemberUp.class, this::handleMemberUp)
                .match(ClusterEvent.MemberRemoved.class, this::handleMemberRemoved)
                .match(ClusterEvent.UnreachableMember.class, this::handleUnreachableMember)
                .match(ClusterEvent.ReachableMember.class, this::handleReachableMember)
                .match(ClusterEvent.MemberExited.class, this::handleMemberExited)
                .match(QueryClusterStatus.class, this::handleQueryClusterStatus)
                .matchAny(msg -> logger.debug("Received cluster event: {}", msg))
                .build();
    }

    /**
     * Handle query cluster status request
     * Returns current cluster members info to the sender
     */
    private void handleQueryClusterStatus(QueryClusterStatus msg) {
        List<ActorSystemStatus.ClusterMemberInfo> members = new ArrayList<>();
        for (Member member : clusterMembers) {
            ActorSystemStatus.ClusterMemberInfo info = new ActorSystemStatus.ClusterMemberInfo();
            info.setAddress(member.address().toString());
            info.setRoles(String.join(",", member.getRoles()));
            info.setStatus(unreachableMembers.contains(member) ? "UNREACHABLE" : member.status().toString());
            info.setUpSince(member.upNumber());
            members.add(info);
        }
        getSender().tell(members, getSelf());
    }

    /**
     * 处理节点上线事件
     *
     * 实现步骤：
     * 1. 记录节点信息到集群成员列表
     * 2. 记录日志
     * 3. 触发任务重新平衡（可选）
     * 4. 通知其他组件（如 NodeDispatcherActor）
     *
     * 应用场景：
     * - 新节点加入集群
     * - 节点从不可达状态恢复
     * - 集群扩容
     *
     * @param event 节点上线事件
     */
    private void handleMemberUp(ClusterEvent.MemberUp event) {
        Member member = event.member();
        logger.info("Member is Up: address={}, roles={}", member.address(), member.getRoles());

        // TODO: 实现节点上线处理

        // 1. 记录节点信息
        clusterMembers.add(member);
        logger.info("Cluster members count: {}", clusterMembers.size());

        // 2. 从不可达列表中移除（如果存在）
        unreachableMembers.remove(member);

        // 3. 触发任务重新平衡（可选）
        // 说明：当新节点加入时，可以将部分任务迁移到新节点
        // 这需要与 NodeDispatcherActor 协作
        // if (shouldRebalance()) {
        //     ActorRef nodeDispatcher = getContext().actorSelection("/user/node-dispatcher").anchor();
        //     nodeDispatcher.tell(new RebalanceTasks(), getSelf());
        // }

        // 4. 记录集群拓扑信息
        logClusterTopology();
    }

    /**
     * 处理节点下线事件
     *
     * 实现步骤：
     * 1. 从集群成员列表中移除
     * 2. 清理相关资源
     * 3. 触发故障恢复（恢复该节点上的任务）
     * 4. 记录日志
     *
     * 应用场景：
     * - 节点正常退出集群
     * - 节点被强制移除
     * - 集群缩容
     *
     * @param event 节点下线事件
     */
    private void handleMemberRemoved(ClusterEvent.MemberRemoved event) {
        Member member = event.member();
        logger.info("Member is Removed: address={}, roles={}", member.address(), member.getRoles());

        // TODO: 实现节点下线处理

        // 1. 从集群成员列表中移除
        clusterMembers.remove(member);
        unreachableMembers.remove(member);
        logger.info("Cluster members count: {}", clusterMembers.size());

        // 2. 触发故障恢复
        // 说明：需要恢复该节点上运行的所有任务
        // String workerAddress = member.address().toString();
        // recoverTasksFromFailedNode(workerAddress);

        // 3. 清理相关资源
        // 说明：清理该节点相关的缓存、连接等
        // cleanupNodeResources(member);

        // 4. 记录集群拓扑信息
        logClusterTopology();
    }

    /**
     * 处理节点不可达事件
     *
     * 实现步骤：
     * 1. 标记节点为不可达
     * 2. 记录告警日志
     * 3. 触发故障恢复（恢复该节点上的任务）
     * 4. 等待节点恢复或被移除
     *
     * 应用场景：
     * - 网络分区
     * - 节点宕机
     * - 节点负载过高无响应
     *
     * 注意：
     * - 不可达不等于下线，节点可能会恢复
     * - 需要配合 Split Brain Resolver 处理脑裂
     * - 故障恢复需要考虑任务幂等性
     *
     * @param event 节点不可达事件
     */
    private void handleUnreachableMember(ClusterEvent.UnreachableMember event) {
        Member member = event.member();
        logger.warn("Member detected as unreachable: address={}, roles={}", member.address(), member.getRoles());

        // TODO: 实现节点不可达处理

        // 1. 标记节点为不可达
        unreachableMembers.add(member);
        logger.warn("Unreachable members count: {}", unreachableMembers.size());

        // 2. 触发故障恢复
        // 说明：需要恢复该节点上运行的所有任务
        // 注意：任务可能还在运行，需要考虑幂等性
        // String workerAddress = member.address().toString();
        // recoverTasksFromUnreachableNode(workerAddress);

        // 3. 发送告警通知（可选）
        // 说明：通知运维人员节点不可达
        // sendAlert("Node unreachable: " + member.address());

        // 4. 记录集群拓扑信息
        logClusterTopology();
    }

    /**
     * 处理节点可达事件
     *
     * 当不可达的节点恢复时触发
     *
     * @param event 节点可达事件
     */
    private void handleReachableMember(ClusterEvent.ReachableMember event) {
        Member member = event.member();
        logger.info("Member is reachable again: address={}, roles={}", member.address(), member.getRoles());

        // 从不可达列表中移除
        unreachableMembers.remove(member);
        logger.info("Unreachable members count: {}", unreachableMembers.size());

        // 记录集群拓扑信息
        logClusterTopology();
    }

    /**
     * 处理节点退出事件
     *
     * 节点正在退出集群（还未完全移除）
     *
     * @param event 节点退出事件
     */
    private void handleMemberExited(ClusterEvent.MemberExited event) {
        Member member = event.member();
        logger.info("Member is exiting: address={}, roles={}", member.address(), member.getRoles());
    }

    /**
     * 恢复失败节点上的任务
     *
     * 当节点下线或不可达时，需要恢复该节点上运行的任务
     *
     * 实现步骤：
     * 1. 查询该节点上运行的所有任务
     * 2. 将任务状态标记为失败
     * 3. 通知 WorkflowInstanceActor 重新调度
     *
     * @param workerAddress Worker 地址
     */
    private void recoverTasksFromFailedNode(String workerAddress) {
        logger.info("Recovering tasks from failed node: {}", workerAddress);

        // TODO: 实现任务恢复逻辑
        // 1. 查询该节点上运行的所有任务
        // List<DAGNodeExecution> runningTasks = dagNodeExecutionDAO.selectRunningNodesByWorker(workerAddress);
        // logger.info("Found {} running tasks on failed node", runningTasks.size());

        // 2. 将任务状态标记为失败
        // for (DAGNodeExecution task : runningTasks) {
        //     task.setStatus(InstanceStatus.FAILED.getDesc());
        //     task.setErrorMsg("Worker node failed: " + workerAddress);
        //     dagNodeExecutionDAO.updateStatus(task);
        // }

        // 3. 通知 DAGSchedulerActor 重新调度
        // ActorRef dagScheduler = getContext().actorSelection("/user/dag-scheduler").anchor();
        // for (DAGNodeExecution task : runningTasks) {
        //     NodeCompleted failureMsg = new NodeCompleted(
        //         task.getWorkflowInstanceId(),
        //         task.getNodeId(),
        //         InstanceStatus.FAILED.getDesc(),
        //         "Worker node failed: " + workerAddress
        //     );
        //     dagScheduler.tell(failureMsg, getSelf());
        // }

        logger.info("Task recovery completed for failed node: {}", workerAddress);
    }

    /**
     * 恢复不可达节点上的任务
     *
     * 与 recoverTasksFromFailedNode 类似，但需要考虑节点可能恢复的情况
     *
     * @param workerAddress Worker 地址
     */
    private void recoverTasksFromUnreachableNode(String workerAddress) {
        logger.warn("Recovering tasks from unreachable node: {}", workerAddress);

        // TODO: 实现任务恢复逻辑
        // 注意：不可达节点可能会恢复，需要考虑任务幂等性
        // 可以等待一段时间（如 30 秒）再触发恢复
        // 或者使用更保守的策略，等待节点被正式移除后再恢复

        logger.warn("Task recovery for unreachable node is pending: {}", workerAddress);
    }

    /**
     * 清理节点资源
     *
     * @param member 集群成员
     */
    private void cleanupNodeResources(Member member) {
        logger.info("Cleaning up resources for node: {}", member.address());

        // TODO: 实现资源清理逻辑
        // 1. 清理该节点相关的缓存
        // 2. 关闭与该节点的连接
        // 3. 清理临时数据

        logger.info("Resource cleanup completed for node: {}", member.address());
    }

    /**
     * 判断是否需要任务重新平衡
     *
     * @return true 如果需要重新平衡
     */
    private boolean shouldRebalance() {
        // TODO: 实现重新平衡判断逻辑
        // 1. 检查集群成员数量变化
        // 2. 检查各节点负载情况
        // 3. 根据策略决定是否重新平衡

        return false;
    }

    /**
     * 记录集群拓扑信息
     *
     * 用于监控和调试
     */
    private void logClusterTopology() {
        logger.info("=== Cluster Topology ===");
        logger.info("Total members: {}", clusterMembers.size());
        logger.info("Unreachable members: {}", unreachableMembers.size());

        // 记录所有成员
        for (Member member : clusterMembers) {
            logger.info("  - Member: address={}, roles={}, status=UP",
                    member.address(), member.getRoles());
        }

        // 记录不可达成员
        for (Member member : unreachableMembers) {
            logger.info("  - Member: address={}, roles={}, status=UNREACHABLE",
                    member.address(), member.getRoles());
        }

        logger.info("========================");
    }

    /**
     * 获取集群状态信息
     *
     * 提供给外部查询使用
     *
     * @return 集群状态信息
     */
    public ClusterStatus getClusterStatus() {
        // TODO: 实现集群状态查询
        // ClusterStatus status = new ClusterStatus();
        // status.setTotalMembers(clusterMembers.size());
        // status.setUnreachableMembers(unreachableMembers.size());
        // status.setLeaderAddress(cluster.state().getLeader() != null ? cluster.state().getLeader().toString() : null);
        // status.setSelfAddress(cluster.selfAddress().toString());
        // return status;

        return null;
    }

    /**
     * 集群状态信息类
     */
    public static class ClusterStatus {
        private int totalMembers;
        private int unreachableMembers;
        private String leaderAddress;
        private String selfAddress;

        // Getters and Setters
        public int getTotalMembers() {
            return totalMembers;
        }

        public void setTotalMembers(int totalMembers) {
            this.totalMembers = totalMembers;
        }

        public int getUnreachableMembers() {
            return unreachableMembers;
        }

        public void setUnreachableMembers(int unreachableMembers) {
            this.unreachableMembers = unreachableMembers;
        }

        public String getLeaderAddress() {
            return leaderAddress;
        }

        public void setLeaderAddress(String leaderAddress) {
            this.leaderAddress = leaderAddress;
        }

        public String getSelfAddress() {
            return selfAddress;
        }

        public void setSelfAddress(String selfAddress) {
            this.selfAddress = selfAddress;
        }
    }
}

