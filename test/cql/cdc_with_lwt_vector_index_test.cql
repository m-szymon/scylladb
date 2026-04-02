-- Reproduces SCYLLADB-1342: LWT writes don't appear in CDC for vector search.
-- Mirror of cdc_with_lwt_test.cql but with a vector index instead of explicit CDC options.
-- The vector index implicitly enables CDC. We verify that LWT writes (insert/update/delete)
-- emit CDC log entries, and failed LWT writes do not.

-- Error messages contain a keyspace name. Make the output stable.
CREATE KEYSPACE ks
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND
    tablets = {'enabled': true};

CREATE TABLE ks.tbl_cdc_lwt_vi (pk int, ck int, val int, v vector<float, 3>, PRIMARY KEY(pk, ck));

-- Creating a vector index implicitly enables CDC on the table.
CREATE INDEX ON ks.tbl_cdc_lwt_vi (v) USING 'vector_index';

-- (0) successful insert
INSERT INTO ks.tbl_cdc_lwt_vi (pk, ck, val, v) VALUES (1, 1, 111, [1.0, 1.0, 1.0]) IF NOT EXISTS;
-- (1) unsuccessful insert
INSERT INTO ks.tbl_cdc_lwt_vi (pk, ck, val, v) VALUES (1, 1, 222, [2.0, 2.0, 2.0]) IF NOT EXISTS;
-- (2) successful update
UPDATE ks.tbl_cdc_lwt_vi SET val = 333, v = [3.0, 3.0, 3.0] WHERE pk = 1 AND ck = 1 IF EXISTS;
-- (3) unsuccessful update
UPDATE ks.tbl_cdc_lwt_vi SET val = 444, v = [4.0, 4.0, 4.0] WHERE pk = 888 AND ck = 777 IF EXISTS;
-- (4) successful row delete
DELETE FROM ks.tbl_cdc_lwt_vi WHERE pk = 1 AND ck = 1 IF EXISTS;
-- (5) unsuccessful row delete
DELETE FROM ks.tbl_cdc_lwt_vi WHERE pk = 1 AND ck = 1 IF EXISTS;

SELECT "cdc$batch_seq_no", "cdc$operation", ck, pk, val, v FROM ks.tbl_cdc_lwt_vi_scylla_cdc_log;

-- there should be 3 rows in total: (0) + (2) + (4)
-- (no preimage since CDC was auto-enabled by vector index with default options)
SELECT count(*) FROM ks.tbl_cdc_lwt_vi_scylla_cdc_log;

-- cleanup
DROP KEYSPACE ks;
