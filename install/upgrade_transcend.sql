SET echo off
SET verify off
SET serveroutput on size unlimited
SET timing off

prompt This script is only for upgrading from 2.6.x versions.
PROMPT If you are upgrading from a version prior to 2.6,
PROMPT then the only current supported upgrade path is a reinstall
PROMPT using the "install_transcend.sql" script
prompt ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DEFINE product = 'transcend'

ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
DEFINE suffix = _&_DATE..log
SPOOL upgrade_&product&suffix

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
@../plsql/specs/TD_ADM.pks
@../plsql/wrapped_bodies/TD_ADM.plb


-- now, recompile objects for the specific Transcend application

-- set the current schema to the application schema
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema=&app_schema';
END;
/

BEGIN
   INSERT INTO column_type_list (column_type) VALUES ('audit');
EXCEPTION
   WHEN dup_val_on_index
    THEN NULL;
END;
/

-- evolve specs
@../evolve/plsql/specs/TD_UTILS.pks
@../evolve/plsql/specs/EVOLVE.pks

-- evolve bodies
@../evolve/plsql/wrapped_bodies/TD_UTILS.plb
@../evolve/plsql/wrapped_bodies/EVOLVE.plb

-- transcend specs
@../transcend/plsql/specs/TRANS_ETL.pks
@../transcend/plsql/specs/TRANS_ADM.pks
@../transcend/plsql/specs/TD_DBUTILS.pks

-- transcend bodes
@../transcend/plsql/wrapped_bodies/MAPPING_OT.plb
@../transcend/plsql/wrapped_bodies/DIMENSION_OT.plb
@../transcend/plsql/wrapped_bodies/TRANS_ETL.plb
@../transcend/plsql/wrapped_bodies/TRANS_ADM.plb
@../transcend/plsql/wrapped_bodies/TD_DBUTILS.plb
@../transcend/plsql/wrapped_bodies/FEED_OT.plb

UPDATE applications
   SET version = 2.642
 WHERE application_name = upper('&app_schema');

UPDATE repositories
   SET version = 2.642
 WHERE repository_name = upper('&app_schema');

EXEC trans_adm.set_default_configs;

-- set the current schema back 
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:b_current_schema;
END;
/

COMMIT;
