DROP TABLE count_table purge
/

CREATE TABLE count_table
(
  entry_ts       TIMESTAMP DEFAULT systimestamp NOT null,
  client_info    VARCHAR2(64) NOT null,
  module         VARCHAR2(48) NOT null,
  action         VARCHAR2(32) NOT null,
  runmode VARCHAR2(10) NOT NULL,
  session_id     NUMBER NOT null,
  row_cnt        NUMBER NOT null
)
/

ALTER TABLE count_table ADD CONSTRAINT count_tbl_pk PRIMARY KEY (session_id, entry_ts)
    USING INDEX
/

GRANT SELECT ON count_table TO applog
/