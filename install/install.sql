SET serveroutput on size unlimited
SET echo off

ACCEPT schema char default 'TDINC' prompt 'Schema to install Transcend in [TDINC]: '
ACCEPT tablespace char default 'TDINC' prompt 'Tablespace to install Transcend in [TDINC]: '

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
GRANT CREATE ANY directory TO &schema;
GRANT EXECUTE ON sys.utl_mail TO &schema;

CREATE ROLE coreutils;
CREATE ROLE transcend;
CREATE ROLE applog;

ALTER USER &schema DEFAULT TABLESPACE &tablespace;
ALTER SESSION SET current_schema=&schema;

SET echo on

--first create framework tables
@../ddl/COUNT_TABLE_TBL.sql
@../ddl/DIR_LIST_TBL.sql
@../ddl/ERR_CD_TBL.sql
@../ddl/FILEHUB_CONF_TBL.sql
@../ddl/FILEHUB_DETAIL_TBL.sql
@../ddl/FILEHUB_OBJ_DETAIL_TBL.sql
@../ddl/LOGGING_CONF_TBL.sql
@../ddl/LOG_TABLE_TBL.sql
@../ddl/NOTIFY_CONF_TBL.sql
@../ddl/PARTNAME_TBL.sql
@../ddl/REGISTRATION_CONF_TBL.sql
@../ddl/RUNMODE_CONF_TBL.sql

--CREATE java stored procedure
@../java/CoreUtils.jvs

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
@../plsql/specs/COREUTILS.pks
@../plsql/wrapped_bodies/COREUTILS.plb

--CREATE targeted types and object views
@../plsql/specs/BASETYPE.tps
@../plsql/wrapped_bodies/BASETYPE.plb
@../plsql/specs/APPLOG.tps
@../plsql/wrapped_bodies/APPLOG.plb
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
@../plsql/specs/TRANSCEND.pks
@../plsql/wrapped_bodies/TRANSCEND.plb
@../plsql/specs/CONTROL.pks
@../plsql/wrapped_bodies/CONTROL.plb

--PUBLIC synonyms
CREATE OR REPLACE PUBLIC SYNONYM transcend FOR &schema.transcend;
CREATE OR REPLACE PUBLIC SYNONYM stragg FOR &schema.stragg;

CREATE OR REPLACE PUBLIC SYNONYM log_table FOR &schema.log_table;
CREATE OR REPLACE PUBLIC SYNONYM count_table FOR &schema.count_table;
CREATE OR REPLACE PUBLIC SYNONYM filehub_detail FOR &schema.filehub_detail;
CREATE OR REPLACE PUBLIC SYNONYM filehub_obj_detail FOR &schema.filehub_obj_detail;

--java permissions
EXEC dbms_java.set_output(1000000);
EXEC dbms_java.grant_permission( upper('&schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&schema'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&schema'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );

ALTER SESSION SET current_schema=&_USER;