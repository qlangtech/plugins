## path
  
  远程FTP文件系统的路径信息，注意这里可以支持填写多个路径。
  
  当指定单个远程FTP文件，FtpReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。
  
  当指定多个远程FTP文件，FtpReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。
  
  当指定通配符，FtpReader尝试遍历出多个文件信息。例如: 指定`/*`代表读取/目录下所有的文件，指定`/bazhen/*`代表读取bazhen目录下游所有的文件。FtpReader目前只支持*作为文件通配符。
  
  特别需要注意的是，DataX会将一个作业下同步的所有Text File视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。读取文件用户必须保证为类CSV格式，并且提供给DataX权限可读。
  
  特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错
  
## column

 描述：读取字段列表，type指定源数据的类型，index指定当前列来自于文本第几列(以0开始)，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。
 
 用户可以指定Column字段信息，配置如下： 
  ```json
   {
     "type": "long" , "index": 0
   }
  ```
  从远程FTP文件文本第一列获取int字段
  ```json
  {
       "type": "long" , "value": "alibaba"
  }
  ``` 
  从FtpReader内部生成`alibaba`的字符串字段作为当前字段

  >> 对于用户指定Column信息，type必须填写，index/value必须选择其一
  
  例子:
  ```json5
  [
   { "index": 0,   "type": "long"  },
   { "index": 1,   "type": "boolean" },
   { "index": 2,   "type": "double" },
   { "index": 3,   "type": "string" },
   { "index": 4,   "type": "date",  "format": "yyyy.MM.dd" },
   { "type": "string", "value": "alibaba"  //从FtpReader内部生成alibaba的字符串字段作为当前字段 
   }
  ]
  ```

## connectPattern

 连接模式（主动模式或者被动模式）。该参数只在传输协议是标准ftp协议时使用，值只能为：**PORT (主动)**，**PASV（被动）**。两种模式主要的不同是数据连接建立的不同。

 1. 对于Port模式，是客户端在本地打开一个端口等服务器去连接建立数据连接，

 2. 而Pasv模式就是服务器打开一个端口等待客户端去建立一个数据连接。
    
## csvReaderConfig

 描述：读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值。
 
 ```json
{ "safetySwitch": false,  
  "skipEmptyRecords": false,       
  "useTextQualifier": false} 
 ```
 所有配置项及默认值,配置时 csvReaderConfig 的map中请严格按照以下字段名字进行配置：
 ```java
 boolean caseSensitive = true;
 char textQualifier = 34;
 boolean trimWhitespace = true;
 boolean useTextQualifier = true;//是否使用csv转义字符
 char delimiter = 44;//分隔符
 char recordDelimiter = 0;
 char comment = 35;
 boolean useComments = false;
 int escapeMode = 1;
 boolean safetySwitch = true;//单列长度是否限制100000字符
 boolean skipEmptyRecords = true;//是否跳过空行
 boolean captureRawRecord = true;
 ```

## nullFormat

 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。
 例如如果用户配置: nullFormat:"\N"，那么如果源头数据是"\N"，DataX视作null字段。默认值：\N
  