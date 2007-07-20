ACCEPT schema char default 'TDINC' prompt 'Schema to install Transcend in [TDINC]: '

DROP ROLE tdinc_coreutils;
DROP ROLE tdinc_filehub;
DROP ROLE tdinc_dbflex;
DROP ROLE tdinc_applog;

ALTER SESSION SET current_schema=&schema;

--first create the tdinc tables
DROP TABLE count_table PURGE;

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
@../java/coreutils.jvs

--DROP all types due to inheritance
DROP TYPE tdinc.feed;
DROP TYPE tdinc.extract;
DROP TYPE tdinc.fhconf;
DROP TYPE tdinc.email;
DROP TYPE tdinc.notify;
DROP TYPE tdinc.applog;

--CREATE core pieces
@../plsql/specs/STRING_AGG_TYPE.tps
@../plsql/wrapped_bodies/STRING_AGG_TYPE.plb
@../plsql/wrapped_bodies/STRAGG.plb
@../plsql/specs/COREUTILS.pks
@../plsql/wrapped_bodies/COREUTILS.plb

--CREATE targeted types
@../plsql/specs/BASETYPE.tps
@../plsql/wrapped_bodies/BASETYPE.plb
@../plsql/specs/APPLOG.tps
@../plsql/wrapped_bodies/APPLOG.plb
@../plsql/specs/NOTIFY.tps
@../plsql/specs/EMAIL.tps
@../plsql/wrapped_bodies/EMAIL.plb
@../plsql/specs/FHCONF.tps
@../plsql/wrapped_bodies/FHCONF.plb
@../plsql/specs/EXTRACT.tps
@../plsql/wrapped_bodies/EXTRACT.plb
@../plsql/specs/FEED.tps
@../plsql/wrapped_bodies/FEED.plb

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