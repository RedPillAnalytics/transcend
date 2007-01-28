DROP TABLE tdinc.count_table purge
/

CREATE TABLE tdinc.count_table
(
  entry_ts       TIMESTAMP,
  client_info    VARCHAR2(64),
  module         VARCHAR2(48),
  action         VARCHAR2(32),
  session_id     NUMBER,
  row_cnt        NUMBER
)
TABLESPACE tdinc
/

ALTER TABLE tdinc.count_table ADD CONSTRAINT count_tbl_pk PRIMARY KEY (session_id, entry_ts)
    USING INDEX
    TABLESPACE tdinc
/

GRANT SELECT ON tdinc.count_table TO tdinc_job
/