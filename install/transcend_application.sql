-- grant full execution rights to the repository user
-- will later ask if the schema should be locked
GRANT CONNECT TO &1;
GRANT RESOURCE TO &1;
GRANT ALTER ANY TABLE TO &1;
GRANT ALTER SESSION TO &1;
GRANT EXECUTE ANY PROCEDURE TO &1;
GRANT INSERT ANY TABLE TO &1;
GRANT SELECT ANY dictionary TO &1;
GRANT SELECT ANY TABLE TO &1;
GRANT UPDATE ANY TABLE TO &1;
GRANT ALTER ANY INDEX TO &1;
GRANT CREATE ANY INDEX TO &1;
GRANT DROP ANY INDEX TO &1;
GRANT DROP ANY TABLE TO &1;
GRANT CREATE ANY directory TO &1;
GRANT EXECUTE ON sys.utl_mail TO &1;

-- set current schema
ALTER SESSION SET current_schema=&1;

--CREATE java stored procedure
@../java/TdCore.jvs

--DROP all types due to inheritance
DROP TYPE feedtype;
DROP TYPE extracttype;
DROP TYPE filetype;
DROP TYPE emailtype;
DROP TYPE notifytype;
DROP TYPE tdtype;
DROP TYPE apptype;

--CREATE core pieces
@../plsql/specs/STRING_AGG_TYPE.tps
@../plsql/wrapped_bodies/STRING_AGG_TYPE.plb
@../plsql/wrapped_bodies/STRAGG.plb
@../plsql/specs/TD_EXT.pks
@../plsql/wrapped_bodies/TD_EXT.plb
@../plsql/specs/BASETYPE.tps
@../plsql/wrapped_bodies/BASETYPE.plb

--CREATE targeted types, packages and object views
@../plsql/specs/APPTYPE.tps
@../plsql/wrapped_bodies/APPTYPE.plb
@../plsql/specs/NOTIFYTYPE.tps
@../plsql/specs/EMAILTYPE.tps
@../plsql/wrapped_bodies/EMAILTYPE.plb
@../object_views/EMAIL_OT_vw.sql
@../plsql/specs/TDTYPE.tps
@../plsql/wrapped_bodies/TDTYPE.plb
@../plsql/specs/TD_CORE.pks
@../plsql/wrapped_bodies/TD_CORE.plb
@../plsql/specs/TD_SQL.pks
@../plsql/wrapped_bodies/TD_SQL.plb
@../plsql/specs/FILETYPE.tps
@../plsql/wrapped_bodies/FILETYPE.plb
@../plsql/specs/EXTRACTTYPE.tps
@../plsql/wrapped_bodies/EXTRACTTYPE.plb
@../object_views/EXTRACT_OT_vw.sql
@../plsql/specs/FEEDTYPE.tps
@../plsql/wrapped_bodies/FEEDTYPE.plb
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

-- don't register TD_SQL components
-- that's because DBMS_MONITOR should look for calling modules for tracing
exec td_control.set_registration('td_sql.exec_sql','noregister');
exec td_control.set_registration('td_sql.exec_auto','noregister');

--java permissions
EXEC dbms_java.set_output(1000000);
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );

ALTER SESSION SET current_schema=&_USER;