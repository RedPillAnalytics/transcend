
prompt This script is only for upgrading from 2.6.x versions.
PROMPT If you are upgrading from a version prior to 2.6,
PROMPT then the only current supported upgrade path is a reinstall
PROMPT using the "install_transcend.sql" script
prompt ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SET echo off
SET verify off
SET serveroutput on size unlimited
SET timing off

DEFINE product = 'transcend'

ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
DEFINE suffix = _&_DATE..log
SPOOL _&product&suffix

-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDREP' prompt 'Application schema to upgrade [tdrep]: '

WHENEVER sqlerror exit sql.sqlcode

VARIABLE b_current_schema char(30)
-- grab the current schema
DECLARE
   l_user           all_users.username%TYPE;
BEGIN
   
   -- get the current schema
   SELECT sys_context('USERENV','CURRENT_SCHEMA')
     INTO :b_current_schema
     FROM dual;

END;
/

-- recreate the TD_ADM package
-- set the current schema to the application schema
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema=tdsys';
END;
/

-- recompile the TD_ADM package
@../plsql/wrapped_bodies/TD_ADM.plb

-- now, recompile objects for the specific Transcend application

-- set the current schema to the application schema
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema=&app_schema';
END;
/

--CREATE targeted _ots, packages and object views
@../transcend/plsql/wrapped_bodies/TD_DBUTILS.plb
@../transcend/plsql/wrapped_bodies/FILE_LABEL_OT.plb
@../transcend/plsql/wrapped_bodies/FILE_DETAIL_OT.plb
@../transcend/plsql/wrapped_bodies/EXTRACT_OT.plb
@../transcend/plsql/wrapped_bodies/FEED_OT.plb
@../transcend/plsql/wrapped_bodies/MAPPING_OT.plb
@../transcend/plsql/wrapped_bodies/DIMENSION_OT.plb

-- set the current schema back 
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:b_current_schema;
END;
/
