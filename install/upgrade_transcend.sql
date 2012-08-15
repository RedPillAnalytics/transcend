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

-- Work on the repository changes (where applicable)

-- set the current schema to the application schema
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema=&rep_schema';
END;
/

UPDATE applications
   SET version = 2.72
 WHERE application_name = upper('&app_schema');

UPDATE repositories
   SET version = 2.72
 WHERE repository_name = upper('&rep_schema');

UPDATE users
   SET version = 2.72
 WHERE repository_name = upper('&rep_schema');

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


-- system application account changes
-- recreate the TD_ADM package
-- set the current schema to the system application schema
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
@../evolve/plsql/wrapped_bodies/EVOLVE_OT.plb

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

EXEC trans_adm.set_default_configs;

-- set the current schema back 
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:b_current_schema;
END;
/

COMMIT;
