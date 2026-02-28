# MySQL 数据库迁移工具

一个功能强大的 Java MySQL 数据库迁移工具，可以将一个 MySQL 数据库的所有表结构和数据迁移到另一个 MySQL 数据库中。

## 功能特性

- ✅ 自动读取源数据库的所有表结构
- ✅ 迁移表结构（包括列定义、主键、索引等）
- ✅ 批量迁移数据，支持自定义批次大小
- ✅ 支持迁移前删除目标表
- ✅ 详细的日志记录和进度显示
- ✅ 错误处理和恢复机制
- ✅ 灵活的配置选项
- ✅ **断点续传功能**：迁移中断后可以从上次中断位置继续传输数据

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
│   ├── migration/
│   │   ├── SchemaMigration.java           # 表结构迁移
│   │   └── DataMigration.java             # 数据迁移
│   └── progress/
│       ├── MigrationProgress.java         # 迁移进度模型
│       ├── ProgressDatabase.java          # H2 数据库管理
│       └── ProgressManager.java           # 进度管理器
├── config.properties                      # 配置文件
├── logback.xml                            # 日志配置
├── pom.xml                                # Maven 配置
└── README.md                              # 说明文档
```

## 环境要求

- JDK 11 或更高版本
- Maven 3.6 或更高版本
- MySQL 5.7 或更高版本

## 快速开始

### 1. 配置数据库连接

编辑 `config.properties` 文件，配置源数据库和目标数据库的连接信息：

```properties
# 源数据库配置
source.db.host=localhost
source.db.port=3306
source.db.database=source_database
source.db.username=root
source.db.password=your_source_password

# 目标数据库配置
target.db.host=localhost
target.db.port=3306
target.db.database=target_database
target.db.username=root
target.db.password=your_target_password

# 迁移配置
migration.batch.size=1000
migration.drop.tables=false
migration.create.tables=true
migration.migrate.data=true
migration.continue.on.error=false

# 断点续传配置
migration.enable.resume=true
```

### 2. 编译项目

```bash
mvn clean package
```

### 3. 运行迁移工具

使用默认配置文件：

```bash
java -jar target/mysql-migration-tool-1.0.0.jar
```

或指定配置文件：

```bash
java -jar target/mysql-migration-tool-1.0.0.jar /path/to/config.properties
```

### 4. 查看日志

迁移过程中，日志会同时输出到控制台和 `logs/migration.log` 文件中。

## 配置说明

### 数据库配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `source.db.host` | 源数据库主机地址 | localhost |
| `source.db.port` | 源数据库端口 | 3306 |
| `source.db.database` | 源数据库名称 | mydb |
| `source.db.username` | 源数据库用户名 | root |
| `source.db.password` | 源数据库密码 | password123 |
| `target.db.host` | 目标数据库主机地址 | localhost |
| `target.db.port` | 目标数据库端口 | 3306 |
| `target.db.database` | 目标数据库名称 | mydb_backup |
| `target.db.username` | 目标数据库用户名 | root |
| `target.db.password` | 目标数据库密码 | password123 |

### 迁移配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `migration.batch.size` | 批量插入的批次大小 | 1000 |
| `migration.drop.tables` | 是否在迁移前删除目标表 | false |
| `migration.create.tables` | 是否创建表结构 | true |
| `migration.migrate.data` | 是否迁移数据 | true |
| `migration.continue.on.error` | 遇到错误是否继续 | false |
| `migration.enable.resume` | 是否启用断点续传 | true |

## 断点续传功能

### 功能说明

断点续传功能使用 H2 数据库存储迁移进度，当迁移过程中断（如网络故障、程序崩溃等）时，可以从上次中断的位置继续迁移，避免从头开始。

### 工作原理

1. **进度存储**：使用 H2 数据库（文件存储在项目根目录的 `migration_progress.mv.db`）记录每个表的迁移进度
2. **断点恢复**：程序启动时自动检查是否有未完成的迁移
3. **智能继续**：根据记录的最后迁移 ID，从断点位置继续传输数据
4. **进度追踪**：实时更新迁移进度，每 1000 行或完成时记录一次

### 使用方法

启用断点续传（默认已启用）：

```properties
migration.enable.resume=true
```

禁用断点续传：

```properties
migration.enable.resume=false
```

### 断点续传场景

#### 场景 1：网络中断

迁移过程中网络中断，重新运行程序后会自动从断点继续：

```bash
# 第一次运行（迁移到 50% 时中断）
java -jar target/mysql-migration-tool-1.0.0.jar

# 网络恢复后，再次运行（从 50% 继续）
java -jar target/mysql-migration-tool-1.0.0.jar
```

#### 场景 2：程序崩溃

程序因内存不足或其他原因崩溃，重新运行后继续迁移：

```bash
# 第一次运行（程序崩溃）
java -jar target/mysql-migration-tool-1.0.0.jar

# 修复问题后，再次运行（继续迁移）
java -jar target/mysql-migration-tool-1.0.0.jar
```

#### 场景 3：手动暂停

可以随时停止程序，稍后继续：

```bash
# 第一次运行（手动停止）
java -jar target/mysql-migration-tool-1.0.0.jar
# 按 Ctrl+C 停止

# 稍后继续
java -jar target/mysql-migration-tool-1.0.0.jar
```

### 进度数据库

断点续传功能使用 H2 数据库存储进度，相关文件：

- `migration_progress.mv.db` - H2 数据库主文件
- `migration_progress.trace.db` - H2 数据库跟踪文件（可选）

**注意**：如果需要重新开始迁移，可以删除这些文件。

### 查看迁移进度

程序启动时会自动检测并显示未完成的迁移进度：

```
========================================
检测到未完成的迁移
========================================
========== 迁移进度摘要 ==========
总表数: 3
表: users, 状态: IN_PROGRESS, 进度: 5000/10000 (50.00%)
表: orders, 状态: PENDING, 进度: 0/20000 (0.00%)
表: products, 状态: COMPLETED, 进度: 1000/1000 (100.00%)
----------------------------------
已完成: 1, 进行中: 1, 失败: 0, 待处理: 1
==================================
将从上次中断的位置继续迁移
```

## 使用场景

### 场景 1：完整迁移数据库

将源数据库的所有表和数据完整迁移到目标数据库：

```properties
migration.drop.tables=true
migration.create.tables=true
migration.migrate.data=true
```

### 场景 2：只迁移表结构

只迁移表结构，不迁移数据：

```properties
migration.drop.tables=false
migration.create.tables=true
migration.migrate.data=false
```

### 场景 3：增量数据迁移

目标表已存在，只迁移数据：

```properties
migration.drop.tables=false
migration.create.tables=false
migration.migrate.data=true
```

### 场景 4：大数据量迁移

对于大数据量表，使用较大的批次大小并启用断点续传：

```properties
migration.batch.size=5000
migration.continue.on.error=true
migration.enable.resume=true
```

### 场景 5：网络不稳定环境

在网络不稳定的环境下迁移，启用断点续传确保数据完整性：

```properties
migration.enable.resume=true
migration.continue.on.error=true
```

## 注意事项

1. **数据库权限**：确保数据库用户有足够的权限（SELECT、CREATE、DROP、INSERT 等）
2. **存储空间**：确保目标数据库有足够的存储空间
3. **网络连接**：确保能够连接到源数据库和目标数据库
4. **数据备份**：建议在迁移前备份目标数据库
5. **字符集**：确保源数据库和目标数据库使用相同的字符集
6. **外键约束**：如果表有外键约束，可能需要调整迁移顺序或临时禁用约束
7. **断点续传**：
   - 断点续传依赖表的主键，确保表有主键
   - 进度数据库文件存储在项目根目录，请勿删除（除非需要重新开始）
   - 如果修改了表结构，建议删除进度文件重新迁移
   - 断点续传功能会自动检测未完成的迁移并继续

## 故障排除

### 问题 1：连接数据库失败

**错误信息**：`无法连接到源数据库`

**解决方案**：
- 检查数据库服务是否运行
- 验证主机地址、端口、用户名和密码
- 确认数据库用户有访问权限

### 问题 2：表已存在错误

**错误信息**：`Table 'xxx' already exists`

**解决方案**：
- 设置 `migration.drop.tables=true` 删除已存在的表
- 或设置 `migration.create.tables=false` 跳过表创建

### 问题 3：内存不足

**错误信息**：`java.lang.OutOfMemoryError`

**解决方案**：
- 减小 `migration.batch.size` 的值
- 增加 JVM 内存：`java -Xmx2g -jar ...`

### 问题 4：数据插入失败

**错误信息**：`Duplicate entry for key 'PRIMARY'`

**解决方案**：
- 确认目标表是否为空
- 检查是否有重复的主键
- 设置 `migration.continue.on.error=true` 跳过错误

### 问题 5：断点续传不工作

**错误信息**：无法从断点继续迁移

**解决方案**：
- 确认表有主键
- 检查进度数据库文件是否存在
- 确认 `migration.enable.resume=true`
- 查看日志了解详细错误信息

### 问题 6：进度数据库错误

**错误信息**：`初始化进度数据库失败`

**解决方案**：
- 删除进度数据库文件（`migration_progress.mv.db`）重新开始
- 检查项目目录是否有写入权限
- 确认 H2 数据库依赖已正确安装

## 开发和构建

### 本地开发

```bash
# 克隆项目
git clone <repository-url>
cd mysql_migration_2

# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包项目
mvn clean package
```

### IDE 导入

项目支持主流 IDE：

- IntelliJ IDEA：直接打开 pom.xml
- Eclipse：使用 `mvn eclipse:eclipse` 生成项目文件
- VS Code：安装 Java Extension Pack

## 许可证

本项目采用 MIT 许可证。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 Issue
- 发送邮件

## 更新日志

### v1.1.0 (2024-01-15)

- 新增断点续传功能
- 使用 H2 数据库存储迁移进度
- 支持从断点位置继续迁移
- 添加进度管理器
- 改进错误处理和日志记录

### v1.0.0 (2024-01-01)

- 初始版本发布
- 支持表结构迁移
- 支持数据迁移
- 支持批量处理
- 完整的日志记录