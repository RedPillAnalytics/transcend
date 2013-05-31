SET echo off
SET verify off
SET serveroutput on size unlimited
SET timing off

prompt This script is only for upgrading versions 2.6 and beyond.
PROMPT If you are upgrading from a version prior to 2.6,
PROMPT then the only current supported upgrade path is a reinstall
PROMPT using the "install_transcend.sql" script
prompt ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DEFINE product = 'transcend'

ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
DEFINE suffix = _&_DATE..log
SPOOL upgrade_&product&suffix

-- get the schema for the Transcend application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDREP' prompt 'Schema name for the application [tdrep]: '
-- get the schema for the Transcend repository (tables)
ACCEPT rep_schema char default 'TDREP' prompt 'Schema name for the default repository for this application [tdrep]: '

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

-- Work on the TDSYS schema and application code
ALTER SESSION SET current_schema=tdsys;

UPDATE applications
   SET version = 3.0
 WHERE application_name = upper('&app_schema');

UPDATE repositories
   SET version = 3.0
 WHERE repository_name = upper('&rep_schema');

UPDATE users
   SET version = 3.0
 WHERE repository_name = upper('&rep_schema');

-- system application account changes
-- recreate the TD_ADM package
@../plsql/specs/TD_ADM.pks
@../plsql/wrapped_bodies/TD_ADM.plb

-- make table DDL changes
-- set the current schema to the repository schema
ALTER SESSION SET current_schema=&rep_schema;

BEGIN
   INSERT INTO column_type_list (column_type) VALUES ('audit');
EXCEPTION
   WHEN dup_val_on_index
    THEN NULL;
END;
/

-- remove unneeded primary key
DECLARE
   e_no_pk   EXCEPTION;
   PRAGMA    EXCEPTION_INIT( e_no_pk, -2441 );
BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE results_table DROP PRIMARY KEY';
EXCEPTION
    WHEN e_no_pk 
    THEN 
       NULL;
END;
/

-- remove columns from the dimension_conf table
DECLARE
   e_no_col  EXCEPTION;
   PRAGMA    EXCEPTION_INIT( e_no_col, -904 );
BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE dimension_conf DROP COLUMN char_nvl_default';
   EXECUTE IMMEDIATE 'ALTER TABLE dimension_conf DROP COLUMN date_nvl_default';
   EXECUTE IMMEDIATE 'ALTER TABLE dimension_conf DROP COLUMN number_nvl_default';
EXCEPTION
    WHEN e_no_col 
    THEN 
       NULL;
END;
/

-- add INTERFACE_TYPE to the CDC_ENTITY table
DECLARE
   e_dup_col EXCEPTION;
   PRAGMA    EXCEPTION_INIT( e_dup_col, -1430 );
BEGIN
   EXECUTE IMMEDIATE q'|ALTER TABLE cdc_entity ADD interface_type VARCHAR2 (10) CHECK ( interface_type IN ('view','mview'))|';
EXCEPTION
    WHEN e_dup_col
    THEN 
       NULL;
END;
/

-- now, recompile objects for the specific Transcend application
-- set the application schema
ALTER SESSION SET current_schema=&app_schema;

-- evolve specs
@../evolve/plsql/specs/TD_UTILS.pks
@../evolve/plsql/specs/EVOLVE.pks

-- evolve bodies
@../evolve/plsql/wrapped_bodies/TD_UTILS.plb
@../evolve/plsql/wrapped_bodies/EVOLVE.plb
@../evolve/plsql/wrapped_bodies/EVOLVE_OT.plb

-- transcend specs
@../transcend/plsql/specs/TRANS_ETL.pks
@../transcend/plsql/specs/TRANS_ADM.pks
@../transcend/plsql/specs/TD_DBUTILS.pks
@../transcend/plsql/specs/DIMENSION_OT.tps

-- transcend bodes
@../transcend/plsql/wrapped_bodies/MAPPING_OT.plb
@../transcend/plsql/wrapped_bodies/DIMENSION_OT.plb
@../transcend/plsql/wrapped_bodies/TRANS_ETL.plb
@../transcend/plsql/wrapped_bodies/TRANS_ADM.plb
@../transcend/plsql/wrapped_bodies/TD_DBUTILS.plb
@../transcend/plsql/wrapped_bodies/FEED_OT.plb

EXEC trans_adm.set_default_configs;

-- set the current schema back 
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:b_current_schema;
END;
/

COMMIT;
