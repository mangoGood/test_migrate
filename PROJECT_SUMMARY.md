# MySQL 数据库迁移工具 - 项目总结

## 项目概述

这是一个功能完整的 Java MySQL 数据库迁移工具，能够自动读取源数据库的所有表结构和数据，并将其迁移到目标数据库中。

## 核心功能

### 1. 数据库连接管理
- 支持源数据库和目标数据库的独立配置
- 自动连接测试和错误处理
- 连接池管理

### 2. 元数据读取
- 自动获取所有表列表
- 读取表结构（CREATE TABLE 语句）
- 读取列信息（列名、类型、是否可空、默认值等）
- 识别主键和自增列
- 统计表行数

### 3. 表结构迁移
- 完整迁移表结构
- 支持删除已存在的表
- 自动清理 SQL 语句（移除数据库名前缀、重置 AUTO_INCREMENT）
- 保持索引、约束等完整结构

### 4. 数据迁移
- 批量插入数据，提高性能
- 支持自定义批次大小
- 自动处理各种数据类型
- 实时进度显示
- 完善的错误处理机制

### 5. 日志记录
- 详细的控制台输出
- 文件日志记录（支持日志轮转）
- 迁移进度跟踪
- 错误详细信息

## 项目结构

```
mysql_migration_2/
├── src/main/java/com/migration/
│   ├── Main.java                          # 主程序入口
│   ├── config/
│   │   ├── DatabaseConfig.java            # 数据库配置类
│   │   └── MigrationConfig.java           # 迁移配置类
│   ├── db/
│   │   └── DatabaseConnection.java        # 数据库连接管理
│   ├── model/
│   │   ├── TableInfo.java                 # 表信息模型
│   │   └── ColumnInfo.java                # 列信息模型
│   ├── metadata/
│   │   └── MetadataReader.java            # 元数据读取器
│   └── migration/
│       ├── SchemaMigration.java           # 表结构迁移
│       └── DataMigration.java             # 数据迁移
├── config.properties                      # 配置文件
├── config.example.properties              # 配置文件示例
├── logback.xml                            # 日志配置
├── pom.xml                                # Maven 配置
├── README.md                              # 完整文档
├── QUICKSTART.md                          # 快速开始指南
└── .gitignore                             # Git 忽略文件
```

## 技术栈

- **语言**: Java 11
- **构建工具**: Maven 3.6+
- **数据库**: MySQL 5.7+
- **日志框架**: SLF4J + Logback
- **JDBC 驱动**: MySQL Connector/J 8.0.33

## 使用方法

### 1. 配置数据库

编辑 `config.properties` 文件：

```properties
# 源数据库
source.db.host=localhost
source.db.port=3306
source.db.database=source_db
source.db.username=root
source.db.password=password

# 目标数据库
target.db.host=localhost
target.db.port=3306
target.db.database=target_db
target.db.username=root
target.db.password=password

# 迁移配置
migration.batch.size=1000
migration.drop.tables=false
migration.create.tables=true
migration.migrate.data=true
migration.continue.on.error=false
```

### 2. 编译项目

```bash
mvn clean package
```

### 3. 运行迁移

```bash
java -jar target/mysql-migration-tool-1.0.0.jar
```

## 配置选项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `migration.batch.size` | int | 1000 | 批量插入的批次大小 |
| `migration.drop.tables` | boolean | false | 是否在迁移前删除目标表 |
| `migration.create.tables` | boolean | true | 是否创建表结构 |
| `migration.migrate.data` | boolean | true | 是否迁移数据 |
| `migration.continue.on.error` | boolean | false | 遇到错误是否继续 |

## 使用场景

### 场景 1：完整迁移
迁移所有表结构和数据到新数据库

### 场景 2：结构迁移
只迁移表结构，用于开发环境初始化

### 场景 3：数据迁移
表结构已存在，只迁移数据

### 场景 4：增量迁移
定期迁移新增数据

## 性能特点

- **批量处理**: 支持自定义批次大小，平衡内存和性能
- **流式读取**: 使用 ResultSet 流式读取，避免内存溢出
- **批量插入**: 使用 JDBC 批处理，提高插入效率
- **进度显示**: 实时显示迁移进度，方便监控

## 错误处理

- 连接失败：自动重试和详细错误提示
- 表已存在：可选择删除或跳过
- 数据插入失败：可选择继续或停止
- 内存不足：建议调整批次大小或增加 JVM 内存

## 日志输出

### 控制台输出
- 迁移进度信息
- 表和数据统计
- 错误和警告信息

### 文件日志
- 完整的迁移日志
- 按日期轮转
- 保留 30 天历史

## 安全特性

- 配置文件包含敏感信息，已加入 .gitignore
- 使用参数化查询，防止 SQL 注入
- 密码不在日志中显示

## 扩展性

项目采用模块化设计，易于扩展：

1. **支持其他数据库**: 只需修改 DatabaseConnection 和 MetadataReader
2. **自定义迁移逻辑**: 可扩展 SchemaMigration 和 DataMigration
3. **添加过滤器**: 可在迁移前后添加数据处理逻辑
4. **支持更多配置**: 可扩展 MigrationConfig 类

## 注意事项

1. **数据库权限**: 确保用户有足够的权限（SELECT, CREATE, DROP, INSERT）
2. **存储空间**: 确保目标数据库有足够的存储空间
3. **网络连接**: 确保能够连接到源数据库和目标数据库
4. **数据备份**: 建议在迁移前备份目标数据库
5. **字符集**: 确保源数据库和目标数据库使用相同的字符集
6. **外键约束**: 如果表有外键约束，可能需要调整迁移顺序

## 测试建议

1. 先在测试环境验证
2. 使用小数据量测试
3. 检查迁移后的数据完整性
4. 验证索引和约束是否正确
5. 测试错误恢复机制

## 已知限制

1. 不支持存储过程和函数的迁移
2. 不支持触发器的迁移
3. 不支持视图的迁移
4. 外键约束可能需要手动处理

## 未来改进方向

1. 支持存储过程和函数迁移
2. 支持触发器和视图迁移
3. 支持增量迁移（基于时间戳）
4. 支持数据过滤和转换
5. 支持并行迁移
6. 添加 Web 界面
7. 支持进度恢复

## 贡献指南

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

## 联系方式

如有问题或建议，请通过 Issue 联系。

---

**项目状态**: ✅ 已完成并测试通过
**编译状态**: ✅ 编译成功
**文档状态**: ✅ 文档完整
