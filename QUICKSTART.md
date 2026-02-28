# 快速使用指南

## 5 分钟快速上手

### 步骤 1：准备环境

确保已安装：
- JDK 11+
- Maven 3.6+
- MySQL 5.7+

### 步骤 2：配置数据库

1. 复制配置文件模板：
```bash
cp config.example.properties config.properties
```

2. 编辑 `config.properties`，修改数据库连接信息：
```properties
# 修改为你的源数据库
source.db.host=localhost
source.db.port=3306
source.db.database=my_source_db
source.db.username=root
source.db.password=your_password

# 修改为你的目标数据库
target.db.host=localhost
target.db.port=3306
target.db.database=my_target_db
target.db.username=root
target.db.password=your_password
```

### 步骤 3：编译项目

```bash
mvn clean package
```

### 步骤 4：运行迁移

```bash
java -jar target/mysql-migration-tool-1.0.0.jar
```

### 步骤 5：查看结果

迁移完成后，检查：
- 控制台输出
- `logs/migration.log` 日志文件
- 目标数据库中的表和数据

## 常见使用场景

### 场景 1：首次完整迁移

```properties
migration.drop.tables=true
migration.create.tables=true
migration.migrate.data=true
```

### 场景 2：只迁移结构（用于开发环境初始化）

```properties
migration.drop.tables=true
migration.create.tables=true
migration.migrate.data=false
```

### 场景 3：增量数据迁移（表已存在）

```properties
migration.drop.tables=false
migration.create.tables=false
migration.migrate.data=true
```

## 性能优化建议

### 大数据量迁移

```properties
# 增大批次大小
migration.batch.size=5000

# 遇到错误继续
migration.continue.on.error=true
```

### 小数据量迁移

```properties
# 减小批次大小，减少内存占用
migration.batch.size=500
```

## 故障排查

### 连接失败
检查数据库服务是否运行，防火墙设置是否正确

### 权限不足
确保数据库用户有足够的权限（SELECT, CREATE, DROP, INSERT）

### 内存不足
```bash
# 增加 JVM 内存
java -Xmx2g -jar target/mysql-migration-tool-1.0.0.jar
```

## 获取帮助

查看完整文档：[README.md](README.md)
