## tabPattern

识别分表的正则式，默认识别分表策略为 `(tabname)_\d+` , 如需使用其他分表策略，如带字母[a-z]的后缀则需要用户自定义

`注意`：如输入自定义正则式，表达式中逻辑表名部分，必须要用括号括起来，不然无法从物理表名中抽取出逻辑表名。

**可参考**：https://github.com/qlangtech/tis/issues/361

## nodeDesc

将分布在多个数据库冗余节点中的物理表视作一个逻辑表，在数据同步管道中进行配置，输入框中可输入以下内容：

* `192.168.28.200[00-07]` ： 单节点多库，导入 192.168.28.200:3306 节点的 order00,order01,order02,order03,order04,order05,order06,order078个库。也可以将节点描述写成：`192.168.28.200[0-7]`，则会导入 192.168.28.200:3306 节点的 order0,order1,order2,order3,order4,order5,order6,order78个库
* `192.168.28.200[00-07],192.168.28.201[08-15]`：会导入 192.168.28.200:3306 节点的 order00,order01,order02,order03,order04,order05,order06,order078个库 和 192.168.28.201:3306 节点的 order08,order09,order10,order11,order12,order13,order14,order158个库，共计16个库

[详细说明](http://tis.pub/docs/guide/datasource/multi-ds-rule)


## prefixWildcardStyle

使用前缀匹配的样式，在flink-cdc表前缀通配匹配的场景中使用
* 选择`是`：在增量监听流程中使用`逻辑表`+`*`的方式对目标表监听，例如，逻辑表名为`base`,启动时使用`base*` 对数据库中 `base01`,`base02`启用增量监听，在运行期用户又增加了`base03`表则执行逻辑会自动对`base03`表开启监听
* 选择`否`：在增量监听流程中使用物理表全匹配的方式进行匹配。在运行期用户增加的新的分表忽略，如需对新加的分表增量监听生效，需要重启增量执行管道。
