DROP TABLE constraint_maint purge
/

CREATE global TEMPORARY TABLE constraint_maint
( 
  table_owner VARCHAR2(30),
  table_name  VARCHAR2(30),
  constraint_name VARCHAR2(30),
  disable_ddl VARCHAR2(2000),
  disable_msg VARCHAR2(2000),
  enable_ddl VARCHAR2(2000),
  enable_msg VARCHAR2(2000)
)
ON COMMIT DELETE ROWS
/
