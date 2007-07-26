PROMPT 'Running transcend_repository.sql'

-- &1 IS the repository schema
DEFINE rep_schema_tr = &1
-- &2 IS the tablespace name
DEFINE tablespace_tr = &2

-- create the user if the user doesn't exist
@@create_rep_user &rep_schema_tr &tablespace_tr

-- give the rep schema a quota on the tablespace
ALTER USER &rep_schema_tr QUOTA 50M ON &tablespace_tr;

-- set the correct schema
ALTER SESSION SET current_schema=&rep_schema_tr;

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
@@rep_grants &rep_schema_tr

ALTER SESSION SET current_schema=&_USER;