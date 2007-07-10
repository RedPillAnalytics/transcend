SET serveroutput on size unlimited
SET echo off

ACCEPT app_schema char default 'TDINC' prompt 'Schema in which to install Transcend packages [TDINC]: '
ACCEPT tab_schema char default 'TDINC' prompt 'Schema in which to install Transcend tables [TDINC]: '
ACCEPT tablespace char default 'TDINC' prompt 'Tablespace in which to install Transcend tables [TDINC]: '

GRANT CONNECT TO &app_schema;
GRANT RESOURCE TO &app_schema;
GRANT ALTER ANY TABLE TO &app_schema;
GRANT ALTER SESSION TO &app_schema;
GRANT EXECUTE ANY PROCEDURE TO &app_schema;
GRANT INSERT ANY TABLE TO &app_schema;
GRANT SELECT ANY dictionary TO &app_schema;
GRANT SELECT ANY TABLE TO &app_schema;
GRANT UPDATE ANY TABLE TO &app_schema;
GRANT ALTER ANY INDEX TO &app_schema;
GRANT CREATE ANY INDEX TO &app_schema;
GRANT DROP ANY INDEX TO &app_schema;
GRANT DROP ANY TABLE TO &app_schema;
GRANT CREATE ANY directory TO &app_schema;
GRANT EXECUTE ON sys.utl_mail TO &app_schema;

CREATE ROLE td_sel;
CREATE ROLE td_adm;

VARIABLE old_tbspace char(30)
DECLARE
   l_app_schema VARCHAR2(30) := '&app_schema';
   l_tab_schema VARCHAR2(30) := '&tab_schema';
BEGIN
   SELECT default_tablespace
     INTO :old_tbspace
     FROM dba_users
    WHERE username=upper('&tab_schema');
   
   IF upper(l_app_schema) <> upper(l_tab_schema)
   THEN
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.COUNT_TABLE for '||l_tab_schema||'.COUNT_TABLE';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.DIR_LIST for '||l_tab_schema||'.DIR_LIST';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.ERR_CD for '||l_tab_schema||'.ERR_CD';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.FILEHUB_CONF for '||l_tab_schema||'.FILEHUB_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.FILEHUB_DETAIL for '||l_tab_schema||'.FILEHUB_DETAIL';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.FILEHUB_OBJ_DETAIL for '||l_tab_schema||'.FILEHUB_OBJ_DETAIL';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.LOGGING_CONF for '||l_tab_schema||'.LOGGING_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.LOG_TABLE for '||l_tab_schema||'.LOG_TABLE';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.NOTIFY_CONF for '||l_tab_schema||'.NOTIFY_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.PARTNAME for '||l_tab_schema||'.PARTNAME';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.REGISTRATION_CONF for '||l_tab_schema||'.REGISTRATION_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.RUNMODE_CONF for '||l_tab_schema||'.RUNMODE_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.PARAMETER_CONF for '||l_tab_schema||'.PARAMETER_CONF';
   END IF;
      
END;
/

ALTER USER &tab_schema DEFAULT TABLESPACE &tablespace;
ALTER SESSION SET current_schema=&tab_schema;

SET echo on

--first create framework tables
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

ALTER SESSION SET current_schema=&app_schema;

--CREATE java stored procedure
@../java/TdCore.jvs

--DROP all types due to inheritance
DROP TYPE feed;
DROP TYPE extract;
DROP TYPE fhconf;
DROP TYPE email;
DROP TYPE notify;
DROP TYPE applog;

--CREATE core pieces
@../plsql/specs/STRING_AGG_TYPE.tps
@../plsql/wrapped_bodies/STRING_AGG_TYPE.plb
@../plsql/wrapped_bodies/STRAGG.plb
@../plsql/wrapped_bodies/GET_ERR_CD.plb
@../plsql/wrapped_bodies/GET_ERR_MSG.plb

--CREATE targeted types, packages and object views
@../plsql/specs/BASETYPE.tps
@../plsql/wrapped_bodies/BASETYPE.plb
@../plsql/specs/APPLOG.tps
@../plsql/wrapped_bodies/APPLOG.plb
@../plsql/specs/TD_CORE.pks
@../plsql/wrapped_bodies/TD_CORE.plb
@../plsql/specs/NOTIFY.tps
@../plsql/specs/EMAIL.tps
@../plsql/wrapped_bodies/EMAIL.plb
@../object_views/EMAIL_OT_vw.sql
@../plsql/specs/FHCONF.tps
@../plsql/wrapped_bodies/FHCONF.plb
@../plsql/specs/EXTRACT.tps
@../plsql/wrapped_bodies/EXTRACT.plb
@../object_views/EXTRACT_OT_vw.sql
@../plsql/specs/FEED.tps
@../plsql/wrapped_bodies/FEED.plb
@../object_views/FEED_OT_vw.sql

--CREATE callable packages
@../plsql/specs/TD_DBAPI.pks
@../plsql/wrapped_bodies/TD_DBAPI.plb
@../plsql/specs/TD_FILEAPI.pks
@../plsql/wrapped_bodies/TD_FILEAPI.plb
@../plsql/specs/TD_CONTROL.pks
@../plsql/wrapped_bodies/TD_CONTROL.plb
@../plsql/specs/TD_OWBAPI.pks
@../plsql/wrapped_bodies/TD_OWBAPI.plb

-- set the default logging, registration and runmodes
EXEC td_control.set_logging_level('default',2,3);
EXEC td_control.set_runmode;
EXEC td_control.set_registration;

--PUBLIC synonyms
CREATE OR REPLACE PUBLIC SYNONYM td_dbapi FOR td_dbapi;
CREATE OR REPLACE PUBLIC SYNONYM td_owbapi FOR td_owbapi;
CREATE OR REPLACE PUBLIC SYNONYM td_fileapi FOR td_fileapi;
CREATE OR REPLACE PUBLIC SYNONYM stragg FOR stragg;

CREATE OR REPLACE PUBLIC SYNONYM log_table FOR log_table;
CREATE OR REPLACE PUBLIC SYNONYM count_table FOR count_table;
CREATE OR REPLACE PUBLIC SYNONYM filehub_detail FOR filehub_detail;
CREATE OR REPLACE PUBLIC SYNONYM filehub_obj_detail FOR filehub_obj_detail;

--java permissions
EXEC dbms_java.set_output(1000000);
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );

ALTER SESSION SET current_schema=&_USER;

BEGIN
   EXECUTE IMMEDIATE 'alter user &tab_schema default tablespace '||:old_tbspace;
END;
/