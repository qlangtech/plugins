## resMatcher

到指定目录中获取目标资源文件，TIS已经为您准备了两种类型的匹配器：

* Wildcard：

   通过 `wildcard` 表达式（如：`user*.json`,`"a/b/*"`），到指定目录扫描所有资源文件，如果与`wildcard`匹配，则作为TDFS Reader的目标资源文件，在后续全量导入流程中读取
* ByMeta：

  `Writer TDFS`作为目标Writer，流程中开启了`添加元数据`选项，将源数据的Schema写入到目标文件系统中，后续，以该`TDFS`作为源数据类型则可以依赖该预写入的Schema文件作为数据源的Schema信息，可大大简化流程
