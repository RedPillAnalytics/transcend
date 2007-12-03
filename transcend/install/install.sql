PROMPT 'Running install.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTranscend_&_DATE..log

-- get the CURRENT_SCHEMA
VARIABLE current_schema char(30)
EXEC :current_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- get the schema for the Transcend repository (tables)
ACCEPT rep_schema char default 'TDSYS' prompt 'Schema name for the Transcend default repository [tdsys]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDSYS' prompt 'Tablespace in which to install Transcend default repository: [tdsys]: '
-- get the schema for the Transcend application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDSYS' prompt 'Schema name for the Transcend application [tdsys]: '

WHENEVER sqlerror exit sql.sqlcode

-- create the Transcend repository
EXEC tdsys.td_install.build_transcend_repo( p_schema => '&rep_schema', p_tablespace => '&tablespace');

-- create the Trancend application
EXEC tdsys.td_install.build_transcend_app( p_schema => '&app_schema', p_repository => '&rep_schema');

-- Install the Transcend Pieces

--CREATE targeted _ots, packages and object views
@../plsql/specs/TD_DBUTILS.pks
@../plsql/wrapped_bodies/TD_DBUTILS.plb
@../plsql/specs/FILE_OT.tps
@../plsql/wrapped_bodies/FILE_OT.plb
@../plsql/specs/EXTRACT_OT.tps
@../plsql/wrapped_bodies/EXTRACT_OT.plb
@../object_views/EXTRACT_OV_vw.sql
@../plsql/specs/FEED_OT.tps
@../plsql/wrapped_bodies/FEED_OT.plb
@../object_views/FEED_OV_vw.sql
@../plsql/specs/DIMENSION_OT.tps
@../plsql/wrapped_bodies/DIMENSION_OT.plb
@../object_views/DIMENSION_OV_vw.sql

--CREATE callable packages
@../plsql/specs/TRANS_ETL.pks
@../plsql/wrapped_bodies/TRANS_ETL.plb
@../plsql/specs/TRANS_FILES.pks
@../plsql/wrapped_bodies/TRANS_FILES.plb
@../plsql/specs/TD_OWB.pks
@../plsql/wrapped_bodies/TD_OWB.plb

-- add notification events
EXEC td_control.set_notification_event('audit_file','file too large','File outside size threshholds','The file referenced below is larger than the configured threshhold:');
EXEC td_control.set_notification_event('audit_file','file too small','File outside size threshholds','The file referenced below is smaller than the configured threshhold:');
