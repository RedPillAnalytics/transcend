DROP TABLE build_indexes purge
/

CREATE global TEMPORARY TABLE build_indexes
( index_owner VARCHAR2(30),
  index_name VARCHAR2(30),
  source_owner VARCHAR2(30),
  source_index VARCHAR2(30),
  partitioned VARCHAR2(3),
  uniqueness VARCHAR2(10),
  index_type VARCHAR2(20),
  index_ddl VARCHAR2(4000),
  rename_ddl VARCHAR2(4000),
  rename_msg VARCHAR2(4000)
)
ON COMMIT DELETE ROWS
/
