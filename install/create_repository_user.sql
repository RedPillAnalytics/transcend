SET serveroutput on size unlimited
SET echo off

ACCEPT repo_user char default 'TDINC_USER' prompt 'Username to configure as Transcend repository user [TDINC_USER]:
ACCEPT repo_schema char default 'TDINC' prompt 'Schema containing Transcend repository for this user [TDINC]:

ALTER USER &tab_schema DEFAULT TABLESPACE &tablespace;
ALTER SESSION SET current_schema=&tab_schema;

SET echo on

--first create framework tables
CREATE SYNONYM &repo_user..COUNT_TABLE FOR &repo_schema..COUNT_TABLE;
CREATE SYNONYM &repo_user..DIR_LIST FOR &repo_schema..DIR_LIST;
CREATE SYNONYM &repo_user..ERR_CD FOR &repo_schema..ERR_CD;
CREATE SYNONYM &repo_user..FILEHUB_CONF FOR &repo_schema..FILEHUB_CONF;
CREATE SYNONYM &repo_user..FILEHUB_DETAIL FOR &repo_schema..FILEHUB_DETAIL;
CREATE SYNONYM &repo_user..FILEHUB_OBJ_DETAIL FOR &repo_schema..FILEHUB_OBJ_DETAIL;
CREATE SYNONYM &repo_user..LOGGING_CONF FOR &repo_schema..LOGGING_CONF;
CREATE SYNONYM &repo_user..LOG_TABLE FOR &repo_schema..LOG_TABLE;
CREATE SYNONYM &repo_user..NOTIFY_CONF FOR &repo_schema..NOTIFY_CONF;
CREATE SYNONYM &repo_user..PARTNAME FOR &repo_schema..PARTNAME;
CREATE SYNONYM &repo_user..REGISTRATION_CONF FOR &repo_schema..REGISTRATION_CONF;
CREATE SYNONYM &repo_user..RUNMODE_CONF FOR &repo_schema..RUNMODE_CONF;
CREATE SYNONYM &repo_user..PARAMETER_CONF FOR &repo_schema..PARAMETER_CONF;