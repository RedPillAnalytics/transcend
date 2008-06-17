SET echo off
SET verify off
PROMPT 'Running install.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTranscend_&_DATE..log

-- get the CURRENT_SCHEMA
VARIABLE current_schema char(30)
EXEC :current_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- get the schema for the Transcend repository (tables)
ACCEPT rep_schema char default 'TDSYS' prompt 'Evolve repository schema to use for the Transcend default repository [tdsys]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDSYS' prompt 'Tablespace in which to install Transcend default repository: [tdsys]: '
-- get the schema for the Transcend application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDSYS' prompt 'Evolve application schema for the Transcend application [tdsys]: '
-- find out whether destructive actions are okay
ACCEPT drop_obj char default 'N' prompt 'Do you want to issue DROP TABLE statements for any existing repository tables? [N]: '


WHENEVER sqlerror exit sql.sqlcode

DECLARE
   l_drop BOOLEAN := CASE WHEN REGEXP_LIKE('yes','&drop_obj','i') THEN TRUE ELSE FALSE END;
BEGIN
   -- create the Transcend repository
   tdsys.td_install.build_transcend_repo( p_schema => '&rep_schema', p_tablespace => '&tablespace', p_drop => l_drop);
   -- create the Trancend application
   tdsys.td_install.build_transcend_app( p_schema => '&app_schema', p_repository => '&rep_schema', p_drop => l_drop);
EXCEPTION
   WHEN tdsys.td_install.e_repo_obj_exists
   THEN
   raise_application_error(-20003,'Repository tables exist. Specify ''Y'' when prompted to issue DROP TABLE statements');
END;
/


-- Install the Transcend Pieces

--CREATE targeted _ots, packages and object views
@../plsql/specs/TD_DBUTILS.pks
@../plsql/wrapped_bodies/TD_DBUTILS.plb
@../plsql/specs/FILE_OT.tps
@../plsql/wrapped_bodies/FILE_OT.plb
@../plsql/specs/EXTRACT_OT.tps
@../plsql/wrapped_bodies/EXTRACT_OT.plb
@../plsql/specs/FEED_OT.tps
@../plsql/wrapped_bodies/FEED_OT.plb
@../plsql/specs/MAPPING_OT.tps
@../plsql/wrapped_bodies/MAPPING_OT.plb
@../plsql/specs/DIMENSION_OT.tps
@../plsql/wrapped_bodies/DIMENSION_OT.plb

--CREATE callable packages
@../plsql/specs/TRANS_ADM.pks
@../plsql/wrapped_bodies/TRANS_ADM.plb
@../plsql/specs/TRANS_ETL.pks
@../plsql/wrapped_bodies/TRANS_ETL.plb
@../plsql/specs/TRANS_FILES.pks
@../plsql/wrapped_bodies/TRANS_FILES.plb

-- grant execute on all the callable packages to the _APP role
EXEC tdsys.td_install.grant_transcend_app_privs( p_schema => '&app_schema' );

-- set Evolve configurations specific to Transcend
EXEC trans_adm.set_default_configs;

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:current_schema;
END;
/

SPOOL off