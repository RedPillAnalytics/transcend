DROP TABLE td_build_idx_gtt purge
/

CREATE global TEMPORARY TABLE td_build_idx_gtt
( 
  index_owner 		      VARCHAR2(30),
  index_name 		      VARCHAR2(30),
  src_index_owner 	      VARCHAR2(30),
  src_index_name 	      VARCHAR2(30),
  create_ddl 		      VARCHAR2(4000),
  create_msg 		      VARCHAR2(4000),
  rename_ddl 		      VARCHAR2(4000),
  rename_msg 		      VARCHAR2(4000)
)
ON COMMIT DELETE ROWS
/
