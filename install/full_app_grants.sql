SET serveroutput on size unlimited
SET echo off

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