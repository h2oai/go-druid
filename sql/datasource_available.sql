SELECT count(*) cnt
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME=?
