DROP TABLE efw.count_table purge
/

CREATE TABLE efw.count_table
(
  entry_ts       TIMESTAMP,
  client_info    VARCHAR2(64),
  module         VARCHAR2(48),
  action         VARCHAR2(32),
  session_id     NUMBER,
  row_cnt        NUMBER
)
TABLESPACE efw
/

ALTER TABLE efw.count_table ADD CONSTRAINT count_tbl_pk PRIMARY KEY (session_id, entry_ts)
    USING INDEX
    TABLESPACE efw
/

GRANT SELECT ON efw.count_table TO efw_job
/