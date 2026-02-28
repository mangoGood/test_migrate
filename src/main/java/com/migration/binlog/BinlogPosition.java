package com.migration.binlog;

import java.io.Serializable;

/**
 * Binlog 位置信息类
 * 用于记录 MySQL binlog 的位置，以便实现增量迁移
 */
public class BinlogPosition implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String filename;  // binlog 文件名，如 mysql-bin.000001
    private long position;    // binlog 文件中的位置
    private long timestamp;    // 记录时间戳
    
    public BinlogPosition() {
    }
    
    public BinlogPosition(String filename, long position) {
        this.filename = filename;
        this.position = position;
        this.timestamp = System.currentTimeMillis();
    }
    
    public String getFilename() {
        return filename;
    }
    
    public void setFilename(String filename) {
        this.filename = filename;
    }
    
    public long getPosition() {
        return position;
    }
    
    public void setPosition(long position) {
        this.position = position;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("BinlogPosition{filename='%s', position=%d, timestamp=%d}", 
                           filename, position, timestamp);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BinlogPosition that = (BinlogPosition) obj;
        return position == that.position && 
               filename != null ? filename.equals(that.filename) : that.filename == null;
    }
    
    @Override
    public int hashCode() {
        int result = filename != null ? filename.hashCode() : 0;
        result = 31 * result + (int) (position ^ (position >>> 32));
        return result;
    }
}
