SET serveroutput on size unlimited
SET echo off
WHENEVER sqlerror exit failure

ACCEPT schema char default 'TD_APP' prompt 'Schema in which to install Transcend application and default repository: [TD_APP]: '
ACCEPT tablespace char default 'TD_APP' prompt 'Tablespace in which to install Transcend default repository: [TD_APP]: '

WHENEVER sqlerror continue

GRANT CONNECT TO &schema;
GRANT RESOURCE TO &schema;
GRANT ALTER ANY TABLE TO &schema;
GRANT ALTER SESSION TO &schema;
GRANT EXECUTE ANY PROCEDURE TO &schema;
GRANT INSERT ANY TABLE TO &schema;
GRANT SELECT ANY dictionary TO &schema;
GRANT SELECT ANY TABLE TO &schema;
GRANT UPDATE ANY TABLE TO &schema;
GRANT ALTER ANY INDEX TO &schema;
GRANT CREATE ANY INDEX TO &schema;
GRANT DROP ANY INDEX TO &schema;
GRANT DROP ANY TABLE TO &schema;
GRANT CREATE ANY directory TO &schema;
GRANT EXECUTE ON sys.utl_mail TO &schema;

CREATE ROLE td_sel_&schema;
CREATE ROLE td_adm_&schema;

VARIABLE old_tbspace char(30)
BEGIN
   SELECT default_tablespace
     INTO :old_tbspace
     FROM dba_users
    WHERE username=upper('&schema');   
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