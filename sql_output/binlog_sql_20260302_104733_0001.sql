-- Binlog SQL Export
-- Generated at: 2026-03-02T10:47:33.436056
-- File: binlog_sql_20260302_104733_0001.sql
-- Format: [POSITION] filename:position, [GTID] gtid_value, SQL statement

[POSITION] binlog.000012:4994
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:16
UPDATE myapp_db.test1 SET id = 15, name = 'jack12' WHERE id = 15 AND name = 'jack122';

[POSITION] binlog.000012:5314
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:17
UPDATE myapp_db.test1 SET id = 15, name = 'jack132' WHERE id = 15 AND name = 'jack12';

