--WHENEVER sqlerror exit failure rollback
SET serveroutput on size unlimited
SET echo on

-- DROP TABLESPACE tdinc
--      INCLUDING CONTENTS AND datafiles
-- /

-- CREATE TABLESPACE tdinc
-- DATAFILE '/u01/oradata/tdinc_01.dbf'
-- SIZE 1M
-- AUTOEXTEND ON NEXT 1M MAXSIZE 20M
-- EXTENT MANAGEMENT LOCAL AUTOALLOCATE SEGMENT SPACE MANAGEMENT AUTO;

-- DROP USER tdinc CASCADE
-- /

-- CREATE USER tdinc IDENTIFIED BY td1nc0n1y DEFAULT TABLESPACE tdinc;
-- grant CONNECT to tdinc;
-- grant RESOURCE to tdinc;
-- grant ALTER ANY TABLE to tdinc;
-- grant ALTER SESSION to tdinc;
-- grant EXECUTE ANY PROCEDURE to tdinc;
-- grant GLOBAL QUERY REWRITE to tdinc;
-- grant INSERT ANY TABLE to tdinc;
-- grant SELECT ANY DICTIONARY to tdinc;
-- grant SELECT ANY TABLE to tdinc;
-- grant UPDATE ANY TABLE to tdinc;
-- GRANT ALTER ANY INDEX TO tdinc;
-- GRANT CREATE ANY directory TO tdinc;

-- CREATE ROLE tdinc_coreutils;
-- CREATE ROLE tdinc_filehub;
-- CREATE ROLE tdinc_dbflex;
-- CREATE ROLE tdinc_applog;

--First create the tdinc tables
@./ddl/COUNT_TABLE_tbl
@./ddl/LOG_TABLE_tbl
@./ddl/DIR_LIST_tbl
@./ddl/FILEHUB_CONF_tbl
@./ddl/FILEHUB_DETAIL_tbl
@./ddl/FILEHUB_OBJ_DETAIL_tbl
@./ddl/PARTNAME_tbl.sql
@./ddl/PARAMETER_CONF_tbl.sql
@./ddl/NOTIFY_CONF_tbl.sql
@./ddl/LOGGING_CONF_tbl.sql
@./ddl/RUNMODE_CONF_tbl.sql
@./ddl/REGISTRATION_CONF_tbl.sql
@./ddl/ERR_CD_tbl.sql

--create java stored procedure
@./java/CoreUtils.jvs

--drop all types due to inheritance
DROP TYPE tdinc.feed;
DROP TYPE tdinc.extract;
DROP TYPE tdinc.fhconf;
DROP TYPE tdinc.email;
DROP TYPE tdinc.notify;
DROP TYPE tdinc.applog;

--create core pieces
@./types/STRING_AGG_TYPE.tps
@./types/STRING_AGG_TYPE.tpb
@./plsql/STRAGG.fnc
@./plsql/COREUTILS.pks
@./plsql/COREUTILS.pkb

--create targeted types
@./types/BASETYPE.tps
@./types/BASETYPE.tpb
@./types/APPLOG.tps
@./types/APPLOG.tpb
@./types/NOTIFY.tps
@./types/EMAIL.tps
@./types/EMAIL.tpb
@./types/FHCONF.tps
@./types/FHCONF.tpb
@./types/EXTRACT.tps
@./types/EXTRACT.tpb
@./types/FEED.tps
@./types/FEED.tpb

--create callable packages
@./plsql/DBFLEX.pks
@./plsql/DBFLEX.pkb
@./plsql/FILEHUB.pks
@./plsql/FILEHUB.pkb
@./plsql/CONTROL.pks
@./plsql/CONTROL.pkb

--public synonyms
CREATE OR REPLACE PUBLIC SYNONYM filehub FOR tdinc.filehub;
CREATE OR REPLACE PUBLIC SYNONYM log_table FOR tdinc.log_table;
CREATE OR REPLACE PUBLIC SYNONYM count_table FOR tdinc.count_table;
CREATE OR REPLACE PUBLIC SYNONYM stragg FOR tdinc.stragg;

--java permissions
EXEC dbms_java.set_output(1000000);
EXEC dbms_java.grant_permission( 'TDINC', 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
EXEC dbms_java.grant_permission( 'TDINC', 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
EXEC dbms_java.grant_permission( 'TDINC', 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
EXEC dbms_java.grant_permission( 'TDINC', 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
EXEC dbms_java.grant_permission( 'TDINC', 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
EXEC dbms_java.grant_permission( 'TDINC', 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );
