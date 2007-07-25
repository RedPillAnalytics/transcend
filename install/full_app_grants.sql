-- &1 IS the application SCHEMA

-- create a super role to grant complete power for the entire framework using system privileges
DROP ROLE &1._sys;
DROP ROLE &1._app;
CREATE ROLE &1._sys;
CREATE ROLE &1._app;

-- grant full execution rights to the system role as well as the application schema
-- will have a chance to lock the application schema later
GRANT CONNECT TO &1._sys;
GRANT CONNECT TO &1;
GRANT RESOURCE TO &1._sys;
GRANT RESOURCE TO &1;
GRANT ALTER ANY TABLE TO &1._sys;
GRANT ALTER ANY TABLE TO &1;
GRANT ALTER SESSION TO &1._sys;
GRANT ALTER SESSION TO &1;
GRANT EXECUTE ANY PROCEDURE TO &1._sys;
GRANT EXECUTE ANY PROCEDURE TO &1;
GRANT INSERT ANY TABLE TO &1._sys;
GRANT INSERT ANY TABLE TO &1;
GRANT SELECT ANY dictionary TO &1._sys;
GRANT SELECT ANY dictionary TO &1;
GRANT SELECT ANY TABLE TO &1._sys;
GRANT SELECT ANY TABLE TO &1;
GRANT SELECT ANY SEQUENCE TO &1._sys;
GRANT SELECT ANY SEQUENCE TO &1;
GRANT UPDATE ANY TABLE TO &1._sys;
GRANT UPDATE ANY TABLE TO &1;
GRANT DELETE ANY TABLE TO &1._sys;
GRANT DELETE ANY TABLE TO &1;
GRANT ALTER ANY INDEX TO &1._sys;
GRANT ALTER ANY INDEX TO &1;
GRANT CREATE ANY INDEX TO &1._sys;
GRANT CREATE ANY INDEX TO &1;
GRANT DROP ANY INDEX TO &1._sys;
GRANT DROP ANY INDEX TO &1;
GRANT DROP ANY TABLE TO &1._sys;
GRANT DROP ANY TABLE TO &1;
GRANT CREATE ANY directory TO &1._sys;
GRANT CREATE ANY directory TO &1;
GRANT ANALYZE ANY TO &1._sys;
GRANT ANALYZE ANY TO &1;
GRANT EXECUTE ON sys.utl_mail TO &1._sys;
GRANT EXECUTE ON sys.utl_mail TO &1;

--java permissions for sys role and application user
EXEC dbms_java.set_output(1000000);
EXEC dbms_java.grant_permission( upper('&1._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( upper('&1._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( upper('&1._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( upper('&1._sys'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( upper('&1._sys'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( upper('&1._sys'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );
EXEC dbms_java.grant_permission( upper('&1'), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );
