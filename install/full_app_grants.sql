PROMPT 'Running full_app_grants.sql'
-- &1 IS the application SCHEMA
DEFINE app_schema = &1

-- create a super role to grant complete power for the entire framework using system privileges
DROP ROLE &app_schema._sys;
DROP ROLE &app_schema._app;
CREATE ROLE &app_schema._sys;
CREATE ROLE &app_schema._app;

-- grant full execution rights to the system role as well as the application schema
-- will have a chance to lock the application schema later
GRANT CONNECT TO &app_schema._sys;
GRANT CONNECT TO &app_schema;
GRANT RESOURCE TO &app_schema._sys;
GRANT RESOURCE TO &app_schema;
GRANT ALTER ANY TABLE TO &app_schema._sys;
GRANT ALTER ANY TABLE TO &app_schema;
GRANT ALTER SESSION TO &app_schema._sys;
GRANT ALTER SESSION TO &app_schema;
GRANT EXECUTE ANY PROCEDURE TO &app_schema._sys;
GRANT EXECUTE ANY PROCEDURE TO &app_schema;
GRANT INSERT ANY TABLE TO &app_schema._sys;
GRANT INSERT ANY TABLE TO &app_schema;
GRANT SELECT ANY dictionary TO &app_schema._sys;
GRANT SELECT ANY dictionary TO &app_schema;
GRANT SELECT ANY TABLE TO &app_schema._sys;
GRANT SELECT ANY TABLE TO &app_schema;
GRANT SELECT ANY SEQUENCE TO &app_schema._sys;
GRANT SELECT ANY SEQUENCE TO &app_schema;
GRANT UPDATE ANY TABLE TO &app_schema._sys;
GRANT UPDATE ANY TABLE TO &app_schema;
GRANT DELETE ANY TABLE TO &app_schema._sys;
GRANT DELETE ANY TABLE TO &app_schema;
GRANT ALTER ANY INDEX TO &app_schema._sys;
GRANT ALTER ANY INDEX TO &app_schema;
GRANT CREATE ANY INDEX TO &app_schema._sys;
GRANT CREATE ANY INDEX TO &app_schema;
GRANT DROP ANY INDEX TO &app_schema._sys;
GRANT DROP ANY INDEX TO &app_schema;
GRANT DROP ANY TABLE TO &app_schema._sys;
GRANT DROP ANY TABLE TO &app_schema;
GRANT CREATE ANY directory TO &app_schema._sys;
GRANT CREATE ANY directory TO &app_schema;
GRANT ANALYZE ANY TO &app_schema._sys;
GRANT ANALYZE ANY TO &app_schema;
GRANT EXECUTE ON sys.utl_mail TO &app_schema._sys;
GRANT EXECUTE ON sys.utl_mail TO &app_schema;

--java permissions for sys role and application user
EXEC dbms_java.set_output(1000000);
EXEC dbms_java.grant_permission( upper('&app_schema._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&app_schema._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&app_schema._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&app_schema._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&app_schema._sys'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&app_schema._sys'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );
EXEC dbms_java.grant_permission( upper('&app_schema'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );
