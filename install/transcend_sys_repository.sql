PROMPT 'Running transcend_sys_repository.sql'

-- &1 IS the sys_repository schema
DEFINE sys_schema_tsr = &1
-- &2 IS the tablespace name
DEFINE tablespace_tsr = &2

-- create the user if the user doesn't exist
@@create_rep_user &sys_schema_tsr &tablespace_tsr

-- give the rep schema a quota on the tablespace
ALTER USER &sys_schema_tsr QUOTA 50M ON &tablespace_tr;

-- set the correct schema
ALTER SESSION SET current_schema=&sys_schema_tsr;

-- create Transcend sys_repository tables
@@../ddl/REPOSITORIES_tbl.sql
@@../ddl/APPLICATIONS_tbl.sql
@@../ddl/USERS_tbl.sql

ALTER SESSION SET current_schema=&_USER;