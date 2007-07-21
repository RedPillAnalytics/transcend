SET serveroutput on size unlimited
SET echo off

ACCEPT schema char default 'TDINC' prompt 'Schema to use for Transcend repository [TDINC]: '
ACCEPT tablespace char default 'TDINC' prompt 'Tablespace to use for Transcend repository [TDINC]: '

CREATE ROLE td_sel_&schema;
CREATE ROLE td_adm_&schema;

VARIABLE old_tbspace char(30)
BEGIN
   SELECT default_tablespace
     INTO :old_tbspace
     FROM dba_users
    WHERE username=upper('&tab_schema');
END;
/

ALTER USER &schema DEFAULT TABLESPACE &tablespace;
ALTER SESSION SET current_schema=&schema;

SET echo on

-- create Transcend repository tables
@../ddl/COUNT_TABLE_tbl.sql
@../ddl/DIR_LIST_tbl.sql
@../ddl/ERR_CD_tbl.sql
@../ddl/FILEHUB_CONF_tbl.sql
@../ddl/FILEHUB_DETAIL_tbl.sql
@../ddl/FILEHUB_OBJ_DETAIL_tbl.sql
@../ddl/LOGGING_CONF_tbl.sql
@../ddl/LOG_TABLE_tbl.sql
@../ddl/NOTIFY_CONF_tbl.sql
@../ddl/PARTNAME_tbl.sql
@../ddl/REGISTRATION_CONF_tbl.sql
@../ddl/RUNMODE_CONF_tbl.sql
@../ddl/PARAMETER_CONF_tbl.sql

ALTER SESSION SET current_schema=&_USER;

BEGIN
   EXECUTE IMMEDIATE 'alter user &tab_schema default tablespace '||:old_tbspace;
END;
/