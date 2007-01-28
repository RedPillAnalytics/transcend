DROP TABLESPACE tdinc
     INCLUDING contents
/

CREATE TABLESPACE tdinc
DATAFILE SIZE 1M
AUTOEXTEND ON NEXT 1M MAXSIZE 20M
EXTENT MANAGEMENT LOCAL AUTOALLOCATE SEGMENT SPACE MANAGEMENT AUTO;

DROP USER tdinc CASCADE
/

CREATE USER tdinc IDENTIFIED BY td1nc0n1y DEFAULT TABLESPACE tdinc;
grant CONNECT to tdinc;
grant RESOURCE to tdinc;
grant ALTER ANY TABLE to tdinc;
grant ALTER SESSION to tdinc;
grant EXECUTE ANY PROCEDURE to tdinc;
grant GLOBAL QUERY REWRITE to tdinc;
grant INSERT ANY TABLE to tdinc;
grant SELECT ANY DICTIONARY to tdinc;
grant SELECT ANY TABLE to tdinc;
grant UPDATE ANY TABLE to tdinc;
GRANT ALTER ANY INDEX TO tdinc;

CREATE ROLE tdinc_coreutils;

CREATE ROLE tdinc_filehub;
CREATE ROLE tdinc_filehub;

CREATE ROLE tdinc_dbflex;

PROMPT 'First create the efw tables'
@./ddl/COUNT_TABLE_tbl
@./ddl/LOG_TABLE_tbl
@./ddl/DIR_LIST_tbl
@./ddl/FILEHUB_CONF_tbl
@./ddl/FILEHUB_DETAIL_tbl
@./ddl/FILEHUB_OBJ_DETAIL_tbl
@./ddl/NOTIFICATION_tbl

PROMPT 'create java stored procedures'
@./java/UTIL.jvs

PROMPT 'create types'
@./types/STRING_AGG_TYPE.tps
@./types/STRING_AGG_TYPE.tpb
@./types/APPLOG.tps
@./types/APPLOG.tpb

PROMPT 'create custom aggregate function'
@./plsql/STRAGG.fnc

PROMPT 'create packages'
@./plsql/CORE_UTILS.pks
@./plsql/CORE_UTILS.pkb
@./plsql/DBFLEX.pks
@./plsql/DBFLEX.pkb
@./plsql/FILEHUB.pks
@./plsql/FILEHUB.pkb