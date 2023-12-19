## hostAliases
 启动Pod时会在容器内的hosts文件中添加所输入的内容，例子：
 ``` yaml
  - ip: "127.0.0.1"
    hostnames:
      - "foo.local"
      - "bar.local"
 ```

## namespace

Kubernetes中的Namespace是一种用于在集群内部组织和隔离资源的机制。一个Namespace可以看作是一个虚拟的集群，它将物理集群划分为多个逻辑部分，每个部分都有自己的一组资源（如Pod、Service、ConfigMap等）。

例如：输入框中录入的值为`tis`,该命名空间尚未创建，则可通过输入命令行：`kubectl create namespace tis` 创建。

Kubernetes 默认包含 ：`default` 这个名字空间，以便于你无需创建新的名字空间即可开始使用新集群。