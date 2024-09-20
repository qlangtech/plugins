## tabPattern

识别分表的正则式，默认识别分表策略为 `tabname_\d+` , 如需使用其他分表策略，如带字母[a-z]的后缀则需要用户自定义

## nodeDesc

将分布在多个数据库冗余节点中的物理表视作一个逻辑表，在数据同步管道中进行配置，输入框中可输入以下内容：

* `192.168.28.200[00-07]` ： 单节点多库，导入 192.168.28.200:3306 节点的 order00,order01,order02,order03,order04,order05,order06,order078个库。也可以将节点描述写成：`192.168.28.200[0-7]`，则会导入 192.168.28.200:3306 节点的 order0,order1,order2,order3,order4,order5,order6,order78个库
* `192.168.28.200[00-07],192.168.28.201[08-15]`：会导入 192.168.28.200:3306 节点的 order00,order01,order02,order03,order04,order05,order06,order078个库 和 192.168.28.201:3306 节点的 order08,order09,order10,order11,order12,order13,order14,order158个库，共计16个库

[详细说明](http://tis.pub/docs/guide/datasource/multi-ds-rule)

