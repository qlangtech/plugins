## impower

保证Flink 在Kubernetes（Session / Application）模式下拥有执行所有操作都有相应的权限，如不拥有相应权限则会报以下错误：
```shell script
io.fabric8.kubernetes.client.KubernetesClientException: pods is forbidden: 
User "system:serviceaccount:default:default" cannot watch resource "pods" in API group "" in the namespace "default"
```

如选择：是，执行过程会查看系统是否有 rolebing：tis-flink-manager，如没有，则会在Kubernetes Cluster中执行以下等效语句：
```shell script
kubectl  create clusterrolebinding tis-flink-manager --clusterrole=cluster-admin --serviceaccount=default:default
```
