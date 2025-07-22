## omsStorage

PowerJob 当前支持多套存储（MongoDB、AliyunOSS、MySQL等数据库），接入方可自由选择合适的存储介质。

## coreDS

保存Powerjob server元数据的关系型数据库连接配置，目前支持两种方式：
1. `Embedded`: 由K8S集群启动MySQL类型的Powerjob Server元数据服务，由于容器MySQL持久化存储卷存在丢失风险，请谨慎使用该种方式。
2. `Customized`: 由用户事先部署的MySQL的数据库服务，提供Powerjob Server元数据服务，因该种数据库服务可提供高可用容灾解决方案（推荐使用）
