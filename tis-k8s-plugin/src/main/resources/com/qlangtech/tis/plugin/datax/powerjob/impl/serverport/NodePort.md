## nodePort

NodePort服务是让外部请求直接访问服务的最原始方式，NodePort是在所有的节点上开放指定的端口，所有发送到这个端口的请求都会直接转发到服务中的pod里；

这种方式不足：
1. 一个端口只提供一个服务使用
2. 只能使用30000-32767之间的端口
3. 如果节点/虚拟机的IP地址发送变化，需要人工处理；

所以在生产环境，不推荐这种方式发布服务

## host 

通过执行如下命令：
```shell
kubectl get nodes -o wide
```
得到如下输出结果：
```shell
NAME            STATUS   ROLES           AGE    VERSION   INTERNAL-IP      EXTERNAL-IP   OS-IMAGE                KERNEL-VERSION           CONTAINER-RUNTIME
baisui-test-2   Ready    control-plane   242d   v1.28.3   192.168.28.201   <none>        CentOS Linux 7 (Core)   3.10.0-1127.el7.x86_64   docker://24.0.5
```
可`任选一条`记录的`INTERNAL-IP`填入输入框中