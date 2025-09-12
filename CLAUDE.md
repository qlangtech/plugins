# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是TIS的官方插件仓库，一个基于Maven的多模块Java项目。项目包含多种数据同步插件，支持DataX、Flink CDC、Hudi、StarRocks等多种数据处理引擎。

## 常用构建命令

### 基础构建命令
- 完整构建：`mvn clean install`
- 跳过测试构建：`mvn clean install -Dmaven.test.skip=true`
- 部署：`mvn deploy`

### 特定插件构建
项目提供了多个专用构建脚本：

- **Hudi相关插件**：使用 `pkg.sh` 脚本
  ```bash
  # 构建Hudi相关插件（包含MySQL CDC和Hudi sink）
  mvn install -Dmaven.test.skip=true -pl tis-datax/tis-datax-local-executor,tis-datax/tis-hive-flat-table-builder-plugin,tis-datax/tis-ds-mysql-v5-plugin,tis-datax/tis-datax-hudi-plugin,tis-incr/tis-flink-cdc-mysql-plugin,tis-incr/tis-sink-hudi-plugin -am -Dappname=all
  ```

- **部分模块构建**：使用 `build_partial.sh`
  ```bash
  mvn install -Dappname=all -pl tis-incr/tis-flink-chunjun-oracle-plugin,tis-incr/tis-flink-chunjun-mysql-plugin,tis-datax/tis-ds-mysql-v5-plugin -am
  ```

### Maven Profiles
项目支持多个环境profile：
- `default-emr`：默认EMR环境 (Spark 2.4.4, Hadoop 2.7)
- `aliyun-emr`：阿里云EMR环境 (Spark 3.2.1, Hadoop 3.2)
- `cdh`：Cloudera CDH环境 (Spark 3.2.1, Hadoop 3.0.0-cdh6.3.2)

使用profile构建：`mvn clean install -P aliyun-emr`

### 测试相关
- 运行单个测试：`mvn test -Dtest=ClassName`
- 跳过测试：`mvn install -Dmaven.test.skip=true`
- 项目使用JUnit 4.13进行单元测试，使用Testcontainers进行集成测试

## 项目架构

### 核心模块结构
- **tis-datax/**：DataX相关插件，包含各种数据源和数据目标的连接器
  - MySQL、Oracle、PostgreSQL、StarRocks、Doris、Hudi等数据库插件
  - 本地执行器和远程执行器
- **tis-incr/**：增量同步相关插件，主要基于Flink CDC
- **tis-k8s-plugin/**：Kubernetes集成插件
- **tis-plugin-test/**：测试容器和测试工具
- **tis-e2e/**：端到端测试套件

### 插件系统
项目使用自定义的TPI（TIS Plugin Interface）插件系统：
- 插件打包格式：`.tpi` 文件
- 插件基类：继承自TIS Plugin框架
- 插件配置：使用Maven TPI Plugin (`maven-tpi-plugin`)

### 依赖管理
- 项目版本：4.3.0
- Java版本：1.8
- 主要外部依赖：
  - Hadoop 2.7.3/3.2.1（根据profile）
  - Hive 2.3.1/3.1.3
  - Hudi 0.14.1
  - Flink CDC 3.4.0
  - PowerJob 4.3.6
  - Testcontainers 1.18.3

## 开发指南

### 添加新插件模块
1. 在相应目录下创建新的Maven模块
2. 更新父pom.xml中的`<modules>`部分
3. 插件需要继承TIS插件框架的基类
4. 使用`maven-tpi-plugin`进行插件打包

### 测试策略
- 单元测试：使用JUnit和Mockito
- 集成测试：使用Testcontainers启动真实数据库容器
- 端到端测试：在tis-e2e模块中进行

### 插件开发注意事项
- 插件依赖需要在父pom中的`dependencyManagement`中声明
- 避免在插件中包含TIS核心依赖（使用provided scope）
- 注意classpath依赖排除配置：`maven-tpi-plugin.classpathDependentExcludes`

## 部署和发布

### TPI文件部署
- 使用 `tisasm-maven-plugin` 进行插件打包和部署
- 插件文件扩展名：`.tpi`
- 部署子目录：`tis-plugin`

### 同步工具
- `syn-tpi.sh`：同步TPI文件的脚本
- `copy-2-remote-node.sh`：将插件复制到远程节点

## 环境兼容性

项目需要支持多种大数据环境：
- **标准环境**：Spark 2.4.4 + Hadoop 2.7.3
- **阿里云EMR**：Spark 3.2.1 + Hadoop 3.2 
- **Cloudera CDH**：Spark 3.2.1 + CDH 6.3.2

在开发时需要考虑不同版本的兼容性问题，特别是Hadoop和Spark的API差异。