DROP TABLE td_con_maint_gtt purge
/

CREATE global TEMPORARY TABLE td_con_maint_gtt
( 
  table_owner                 VARCHAR2(30),
  table_name 		      VARCHAR2(30),
  constraint_name 	      VARCHAR2(30),
  disable_ddl 		      VARCHAR2(4000),
  disable_msg 		      VARCHAR2(4000),
  enable_ddl 		      VARCHAR2(4000),
  enable_msg 		      VARCHAR2(4000)
)
ON COMMIT DELETE ROWS
/
