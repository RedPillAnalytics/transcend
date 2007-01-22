DROP USER efw CASCADE
/

PROMPT 'security'

CREATE USER efw IDENTIFIED BY et10n1y DEFAULT TABLESPACE users;
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

CREATE ROLE efw_filemover;
CREATE ROLE efw_filemover_read;

CREATE ROLE efw_extracts;
CREATE ROLE efw_extracts_read;

CREATE ROLE efw_etl;

CREATE ROLE efw_job;

PROMPT 'First create the efw tables'
@./ddl/COUNT_TABLE_tbl.sql
@./ddl/LOG_TABLE_tbl.sql
@./ddl/DIR_LIST_tbl.sql
@./ddl/EXT_TAB_DTL_tbl.sql
@./ddl/FILE_CTL_tbl.sql
@./ddl/FILE_DTL_tbl.sql

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
@./plsql/EXTRACTS.pks
@./plsql/EXTRACTS.pkb
@./plsql/FILE_MOVER.pks
@./plsql/FILE_MOVER.pkb