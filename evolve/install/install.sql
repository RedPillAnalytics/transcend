PROMPT 'Running install.sql'
SET serveroutput on size unlimited
SET echo off
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTranscend_&_DATE..log

-- first get the schema for the Transcend repository (tables) first
ACCEPT rep_schema char default 'TDSYS' prompt 'Schema name for the Transcend default repository [tdsys]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDSYS' prompt 'Tablespace in which to install Transcend default repository: [tdsys]: '
-- get application user
ACCEPT app_schema char default 'TDSYS' prompt 'Schema name for the Transcend application [tdsys]: '

WHENEVER sqlerror exit sql.sqlcode

VARIABLE old_tbspace char(30)
DECLARE
   e_user_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
   e_no_tbspace	 EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'CREATE USER tdsys identified by no2tdsys default tablespace &tablespace';
   EXCEPTION
      WHEN e_user_exists
      THEN
      -- get the current default tablespace of the repository user
      SELECT default_tablespace
	INTO :old_tbspace
	FROM dba_users
       WHERE username=upper('&rep_schema');
      EXECUTE IMMEDIATE 'alter user &rep_schema default tablespace &tablespace';
      WHEN e_no_tbspace
      THEN
      raise_application_error(-20001,'Tablespace &tablespace does not exist');
   END;
END;
/

-- install the installation assistance package
@../plsql/specs/TD_EVOLVE_INSTALL.pks
@../plsql/wrapped_bodies/TD_EVOLVE_INSTALL.plb

-- give the rep schema a quota on the tablespace
ALTER USER tdsys QUOTA 50M ON &tablespace;

SET termout off
-- set the correct schema
ALTER SESSION SET current_schema=tdsys;

-- create Transcend sys_repository tables
@@../ddl/REPOSITORIES_tbl.sql
@@../ddl/APPLICATIONS_tbl.sql
@@../ddl/USERS_tbl.sql

EXEC td_evolve_install.create_user('&rep_user','&tablespace');