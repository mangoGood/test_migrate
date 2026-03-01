-- Binlog SQL Export
-- Generated at: 2026-03-01T12:22:48.523546
-- File: binlog_sql_20260301_122248_0001.sql
-- Format: [POSITION] filename:position, [GTID] gtid_value, SQL statement

[POSITION] binlog.000012:966
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:3
DELETE FROM myapp_db.test1 WHERE id = 5 AND name = 'anne';

[POSITION] binlog.000012:1261
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:4
INSERT INTO myapp_db.test1 (id, name) VALUES (5, 'anne');

[POSITION] binlog.000012:1556
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:5
DELETE FROM myapp_db.test1 WHERE id = 5 AND name = 'anne';

[POSITION] binlog.000012:1851
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:6
INSERT INTO myapp_db.test1 (id, name) VALUES (15, 'aaabc');

[POSITION] binlog.000012:2156
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:7
UPDATE myapp_db.test1 SET id = 1, name = 'jack' WHERE id = 1 AND name = 'abc';

[POSITION] binlog.000012:2470
[GTID] a890051c-eb78-11f0-a60f-dafc9531d26a:8
UPDATE myapp_db.test1 SET id = 15, name = 'jack' WHERE id = 15 AND name = 'aaabc';

