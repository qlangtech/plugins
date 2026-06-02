# Falcon ChatBI 测试数据集使用说明

## 概述

`FalconChatBITestExample` 是用于加载和查询 Falcon 测试数据集的工具类。该数据集包含 309 个测试案例，覆盖 16 个不同的数据库。

## 数据结构

### FalconTestCase
表示单个测试案例，包含以下字段：
- `questionId`: 问题ID
- `dbId`: 数据库ID
- `question`: 中文问题描述
- `sql`: SQL 语句列表（通常包含一个元素）
- `answer`: 答案列表（Map<字段名, List<值>>）
- `isOrder`: 是否对结果顺序敏感（"0"=否，"1"=是）

## 快速开始

### 1. 加载数据集

```java
// 从默认路径加载（/opt/misc/Falcon/dev_data/dev.json）
FalconChatBITestExample example = FalconChatBITestExample.load();

// 或从指定路径加载
FalconChatBITestExample example = FalconChatBITestExample.load("/path/to/dev.json");
```

### 2. 根据 db_id 获取测试案例

```java
// 获取特定数据库的所有测试案例
List<FalconTestCase> testCases = example.getTestCasesByDbId("14");

System.out.println("找到 " + testCases.size() + " 个测试案例");

for (FalconTestCase testCase : testCases) {
    System.out.println("问题: " + testCase.getQuestion());
    System.out.println("SQL: " + testCase.getFirstSql());
    System.out.println();
}
```

### 3. 获取所有 db_id

```java
Set<String> allDbIds = example.getAllDbIds();
System.out.println("所有数据库 ID: " + allDbIds);
// 输出: [14, 20, 16, 28, 4, 8, 24, 17, 21, 19, 15, 26, 18, 5, 7, 9]
```

### 4. 根据 question_id 查找测试案例

```java
Optional<FalconTestCase> testCase = example.getTestCaseByQuestionId(0);
testCase.ifPresent(tc -> {
    System.out.println("问题: " + tc.getQuestion());
    System.out.println("数据库: " + tc.getDbId());
    System.out.println("SQL: " + tc.getFirstSql());
});
```

### 5. 查看统计信息

```java
// 打印完整统计信息
example.printStatistics();

// 或获取具体数据
int totalCount = example.getTotalTestCaseCount();  // 309
int dbCount = example.getDbIdCount();              // 16
Map<String, Integer> countsByDb = example.getTestCaseCountsByDbId();
```

### 6. 打印特定数据库的所有测试案例

```java
example.printTestCasesByDbId("14");
```

## 数据集统计

```
总测试案例数: 309
数据库数量: 16

各数据库测试案例数量:
  db_id=14: 32 个测试案例
  db_id=20: 29 个测试案例
  db_id=16: 16 个测试案例
  db_id=28: 17 个测试案例
  db_id=4:  22 个测试案例
  db_id=8:  16 个测试案例
  db_id=24: 17 个测试案例
  db_id=17: 14 个测试案例
  db_id=21: 35 个测试案例
  db_id=19: 10 个测试案例
  db_id=15: 29 个测试案例
  db_id=26: 24 个测试案例
  db_id=18: 16 个测试案例
  db_id=5:  15 个测试案例
  db_id=7:  9 个测试案例
  db_id=9:  8 个测试案例
```

## 完整示例

```java
import com.qlangtech.tis.plugin.ontology.chatbi.FalconChatBITestExample;
import com.qlangtech.tis.plugin.ontology.chatbi.FalconTestCase;

import java.io.IOException;
import java.util.List;

public class UsageExample {
    public static void main(String[] args) throws IOException {
        // 加载数据集
        FalconChatBITestExample example = FalconChatBITestExample.load();
        
        // 获取 db_id=14 的所有测试案例
        String targetDbId = "14";
        List<FalconTestCase> testCases = example.getTestCasesByDbId(targetDbId);
        
        System.out.println("数据库 " + targetDbId + " 共有 " + testCases.size() + " 个测试案例\n");
        
        // 遍历并处理每个测试案例
        for (FalconTestCase testCase : testCases) {
            System.out.println("=== 测试案例 " + testCase.getQuestionId() + " ===");
            System.out.println("问题: " + testCase.getQuestion());
            System.out.println("SQL: " + testCase.getFirstSql());
            
            // 获取答案（如果需要）
            if (testCase.getAnswer() != null && !testCase.getAnswer().isEmpty()) {
                System.out.println("答案字段: " + testCase.getFirstAnswer().keySet());
            }
            
            System.out.println("是否顺序敏感: " + testCase.isOrderSensitive());
            System.out.println();
        }
    }
}
```

## API 方法列表

### 加载方法
- `static FalconChatBITestExample load()` - 从默认路径加载
- `static FalconChatBITestExample load(String jsonFilePath)` - 从指定路径加载

### 查询方法
- `List<FalconTestCase> getTestCasesByDbId(String dbId)` - 根据 db_id 获取测试案例列表
- `Optional<FalconTestCase> getTestCaseByQuestionId(int questionId)` - 根据 question_id 查找
- `List<FalconTestCase> getAllTestCases()` - 获取所有测试案例
- `Set<String> getAllDbIds()` - 获取所有 db_id

### 统计方法
- `int getTotalTestCaseCount()` - 获取总测试案例数
- `int getDbIdCount()` - 获取数据库数量
- `int getTestCaseCountByDbId(String dbId)` - 获取指定数据库的测试案例数
- `Map<String, Integer> getTestCaseCountsByDbId()` - 获取每个数据库的测试案例数统计

### 打印方法
- `void printStatistics()` - 打印统计信息
- `void printTestCasesByDbId(String dbId)` - 打印指定数据库的所有测试案例

## 测试

运行单元测试：
```bash
cd /Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin
mvn test -Dtest=FalconChatBITestExampleTest
```

## 注意事项

1. 确保 Falcon 数据集文件存在于默认路径：`/opt/misc/Falcon/dev_data/dev.json`
2. 数据集使用 Fastjson 进行反序列化
3. 所有返回的列表都是不可修改的，以保证数据一致性
4. 查询不存在的 db_id 会返回空列表，而不是 null
