CREATE TABLESPACE efw
DATAFILE SIZE 1M
AUTOEXTEND ON NEXT 1M MAXSIZE 20M
EXTENT MANAGEMENT LOCAL AUTOALLOCATE SEGMENT SPACE MANAGEMENT AUTO;

DROP USER efw CASCADE
/

CREATE USER efw IDENTIFIED BY et10n1y DEFAULT TABLESPACE efw;
grant CONNECT to efw;
grant RESOURCE to efw;
grant ALTER ANY TABLE to efw;
grant ALTER SESSION to efw;
grant EXECUTE ANY PROCEDURE to efw;
grant GLOBAL QUERY REWRITE to efw;
grant INSERT ANY TABLE to efw;
grant SELECT ANY DICTIONARY to efw;
grant SELECT ANY TABLE to efw;
grant UPDATE ANY TABLE to efw;

CREATE ROLE efw_utility;

CREATE ROLE efw_util;
n
CREATE ROLE efw_filemover;
CREATE ROLE efw_filemover_read;

CREATE ROLE efw_extracts;
CREATE ROLE efw_extracts_read;

CREATE ROLE efw_etl;

CREATE ROLE efw_job;

PROMPT 'First create the efw tables'
@./ddl/COUNT_TABLE_tbl
@./ddl/LOG_TABLE_tbl
@./ddl/DIR_LIST_tbl
@./ddl/EXT_TAB_DTL_tbl
@./ddl/FILE_CTL_tbl
@./ddl/FILE_DTL_tbl
@./ddl/EXTRACT_CONF_tbl

PROMPT 'create java stored procedures'
@./java/UTIL.jvs

PROMPT 'create types'
@./types/STRING_AGG_TYPE.tps
@./types/STRING_AGG_TYPE.tpb
@./types/APP_INFO.tps
@./types/APP_INFO.tpb

PROMPT 'create custom aggregate function'
@./plsql/STRAGG.fnc

PROMPT 'create packages'
@./plsql/JOB.pks
@./plsql/JOB.pkb
@./plsql/UTILITY.pks
@./plsql/UTILITY.pkb
@./plsql/UTIL.pks
@./plsql/UTIL.pkb
@./plsql/ETL.pks
@./plsql/ETL.pkb
@./plsql/FILE_EXTRACT.pks
@./plsql/FILE_EXTRACT.pkb
@./plsql/FILE_MOVER.pks
@./plsql/FILE_MOVER.pkb