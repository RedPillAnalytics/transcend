DROP TABLE count_table purge
/

CREATE TABLE count_table
(
  entry_ts       TIMESTAMP DEFAULT systimestamp NOT null,
  client_info    VARCHAR2(64),
  module         VARCHAR2(48),
  action         VARCHAR2(32),
  runmode VARCHAR2(10) NOT NULL,
  session_id     NUMBER NOT null,
  row_cnt        NUMBER NOT null
)
/

ALTER TABLE count_table ADD CONSTRAINT count_tbl_pk PRIMARY KEY (session_id, entry_ts)
    USING INDEX
/
