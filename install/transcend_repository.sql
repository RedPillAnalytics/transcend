PROMPT 'Running transcend_repository.sql'

-- &1 IS the repository schema
DEFINE rep_schema = &1
-- &2 IS the tablespace name
DEFINE tablespace = &2

-- create the user if the user doesn't exist
@@create_user &rep_schema &tablespace

-- give the rep schema a quota on the tablespace
ALTER USER &rep_schema QUOTA 50M ON &tablespace;

-- set the correct schema
ALTER SESSION SET current_schema=&rep_schema;

-- create Transcend repository tables
@@../ddl/DIR_LIST_tbl.sql
@@../ddl/COUNT_TABLE_tbl.sql
@@../ddl/ERR_CD_tbl.sql
@@../ddl/FILEHUB_CONF_tbl.sql
@@../ddl/FILEHUB_DETAIL_tbl.sql
@@../ddl/FILEHUB_OBJ_DETAIL_tbl.sql
@@../ddl/LOGGING_CONF_tbl.sql
@@../ddl/LOG_TABLE_tbl.sql
@@../ddl/NOTIFY_CONF_tbl.sql
@@../ddl/PARTNAME_tbl.sql
@@../ddl/REGISTRATION_CONF_tbl.sql
@@../ddl/RUNMODE_CONF_tbl.sql
@@../ddl/PARAMETER_CONF_tbl.sql

-- issue grants to the roles created for this repository
@@rep_grants &rep_schema

ALTER SESSION SET current_schema=&_USER;