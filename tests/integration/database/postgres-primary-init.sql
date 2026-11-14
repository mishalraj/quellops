CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replica-test';
CREATE ROLE monitor WITH LOGIN PASSWORD 'monitor-test';
GRANT pg_monitor TO monitor;
