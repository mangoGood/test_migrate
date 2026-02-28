package com.migration.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.migration.db.DatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Binlog 管理类
 * 负责读取和解析 MySQL binlog，实现增量数据迁移
 */
public class BinlogManager {
    private static final Logger logger = LoggerFactory.getLogger(BinlogManager.class);
    
    private DatabaseConnection sourceConnection;
    private DatabaseConnection targetConnection;
    private BinaryLogClient binaryLogClient;
    private Set<String> includedDatabases;  // 需要迁移的数据库列表
    private Set<String> includedTables;     // 需要迁移的表列表
    private boolean running;
    private BinlogPosition startPosition;
    private BinlogPosition currentPosition;
    
    public BinlogManager(DatabaseConnection sourceConnection, 
                        DatabaseConnection targetConnection,
                        Set<String> includedDatabases,
                        Set<String> includedTables) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.includedDatabases = includedDatabases;
        this.includedTables = includedTables;
        this.running = false;
    }
    
    /**
     * 获取当前 binlog 位置
     */
    public BinlogPosition getCurrentBinlogPosition() throws SQLException {
        String sql = "SHOW MASTER STATUS";
        
        try (var stmt = sourceConnection.getConnection().createStatement();
             var rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                String filename = rs.getString("File");
                long position = rs.getLong("Position");
                BinlogPosition binlogPosition = new BinlogPosition(filename, position);
                logger.info("当前 binlog 位置: {}", binlogPosition);
                return binlogPosition;
            }
        }
        
        throw new SQLException("无法获取当前 binlog 位置");
    }
    
    /**
     * 设置开始位置
     */
    public void setStartPosition(BinlogPosition position) {
        this.startPosition = position;
        logger.info("设置 binlog 开始位置: {}", position);
    }
    
    /**
     * 启动 binlog 监听
     */
    public void start() throws IOException {
        if (running) {
            logger.warn("Binlog 监听已在运行中");
            return;
        }
        
        String host = sourceConnection.getConfig().getHost();
        int port = sourceConnection.getConfig().getPort();
        String username = sourceConnection.getConfig().getUsername();
        String password = sourceConnection.getConfig().getPassword();
        
        binaryLogClient = new BinaryLogClient(host, port, username, password);
        
        // 设置开始位置
        if (startPosition != null) {
            binaryLogClient.setBinlogFilename(startPosition.getFilename());
            binaryLogClient.setBinlogPosition(startPosition.getPosition());
            logger.info("从指定位置开始监听: {}", startPosition);
        }
        
        // 设置事件监听器
        binaryLogClient.registerEventListener(this::handleEvent);
        
        // 设置生命周期监听器
        binaryLogClient.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                logger.info("已连接到 MySQL binlog");
            }
            
            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                logger.error("Binlog 通信失败", ex);
                running = false;
            }
            
            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                logger.error("Binlog 事件反序列化失败", ex);
            }
            
            @Override
            public void onDisconnect(BinaryLogClient client) {
                logger.info("已断开与 MySQL binlog 的连接");
                running = false;
            }
        });
        
        binaryLogClient.connect();
        running = true;
        logger.info("Binlog 监听已启动");
    }
    
    /**
     * 停止 binlog 监听
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        try {
             if (binaryLogClient != null) {
                binaryLogClient.disconnect();
            }
        } catch (IOException e) {
            logger.error("断开 binlog 连接失败", e);
        }
        
        running = false;
        logger.info("Binlog 监听已停止");
    }
    
    /**
     * 处理 binlog 事件
     */
    private void handleEvent(Event event) {
        if (!running) {
            return;
        }
        
        EventType eventType = event.getHeader().getEventType();
        
        // 更新当前位置
        if (event instanceof EventData) {
            EventHeaderV4 header = (EventHeaderV4) event.getHeader();
            currentPosition = new BinlogPosition(
                binaryLogClient.getBinlogFilename(),
                header.getPosition()
            );
        }
        
        // 处理不同类型的事件
        if (eventType == EventType.QUERY) {
            handleQueryEvent((QueryEventData) event.getData());
        } else if (eventType == EventType.TABLE_MAP) {
            handleTableMapEvent((TableMapEventData) event.getData());
        } else if (eventType == EventType.EXT_WRITE_ROWS) {
            handleWriteRowsEvent((WriteRowsEventData) event.getData());
        } else if (eventType == EventType.EXT_UPDATE_ROWS) {
            handleUpdateRowsEvent((UpdateRowsEventData) event.getData());
        } else if (eventType == EventType.EXT_DELETE_ROWS) {
            handleDeleteRowsEvent((DeleteRowsEventData) event.getData());
        }
    }
    
    /**
     * 处理查询事件（如 CREATE, ALTER, DROP 等 DDL）
     */
    private void handleQueryEvent(QueryEventData data) {
        String database = data.getDatabase();
        String sql = data.getSql();
        
        if (!shouldProcessDatabase(database)) {
            return;
        }
        
        logger.debug("处理查询事件: database={}, sql={}", database, sql);
        
        // 执行 DDL 语句到目标数据库
        try {
            targetConnection.execute(sql);
            logger.info("已执行 DDL: {}", sql);
        } catch (SQLException e) {
            logger.error("执行 DDL 失败: {}", sql, e);
        }
    }
    
    /**
     * 处理表映射事件
     */
    private void handleTableMapEvent(TableMapEventData data) {
        String database = data.getDatabase();
        String table = data.getTable();
        
        if (!shouldProcessTable(database, table)) {
            return;
        }
        
        logger.debug("表映射事件: database={}, table={}", database, table);
    }
    
    /**
     * 处理插入行事件
     */
    private void handleWriteRowsEvent(WriteRowsEventData data) {
        // 这里需要从 TableMapEventData 获取表信息
        // 简化处理，实际需要维护表映射关系
        logger.debug("处理插入行事件: tableId={}", data.getTableId());
        
        // TODO: 实现插入数据的逻辑
        // 1. 根据 tableId 获取表名
        // 2. 构建 INSERT 语句
        // 3. 执行到目标数据库
    }
    
    /**
     * 处理更新行事件
     */
    private void handleUpdateRowsEvent(UpdateRowsEventData data) {
        logger.debug("处理更新行事件: tableId={}", data.getTableId());
        
        // TODO: 实现更新数据的逻辑
        // 1. 根据 tableId 获取表名
        // 2. 构建 UPDATE 语句
        // 3. 执行到目标数据库
    }
    
    /**
     * 处理删除行事件
     */
    private void handleDeleteRowsEvent(DeleteRowsEventData data) {
        logger.debug("处理删除行事件: tableId={}", data.getTableId());
        
        // TODO: 实现删除数据的逻辑
        // 1. 根据 tableId 获取表名
        // 2. 构建 DELETE 语句
        // 3. 执行到目标数据库
    }
    
    /**
     * 判断是否应该处理该数据库
     */
    private boolean shouldProcessDatabase(String database) {
        if (database == null || database.isEmpty()) {
            return false;
        }
        
        if (includedDatabases == null || includedDatabases.isEmpty()) {
            return true;  // 如果没有指定数据库，则处理所有数据库
        }
        
        return includedDatabases.contains(database);
    }
    
    /**
     * 判断是否应该处理该表
     */
    private boolean shouldProcessTable(String database, String table) {
        if (!shouldProcessDatabase(database)) {
            return false;
        }
        
        if (includedTables == null || includedTables.isEmpty()) {
            return true;  // 如果没有指定表，则处理所有表
        }
        
        String fullTableName = database + "." + table;
        return includedTables.contains(fullTableName) || includedTables.contains(table);
    }
    
    /**
     * 获取当前位置
     */
    public BinlogPosition getCurrentPosition() {
        return currentPosition;
    }
    
    /**
     * 检查是否正在运行
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * 等待指定时间
     */
    public void waitFor(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        while (running && System.currentTimeMillis() < endTime) {
            Thread.sleep(100);
        }
    }
}
