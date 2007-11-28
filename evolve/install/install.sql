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
      EXECUTE IMMEDIATE 'CREATE USER tdsys identified by no2tdsys';
   EXCEPTION
      WHEN e_user_exists
      THEN
        NULL;
   END;
END;
/

-- needed to interact with users and their tablespaces
GRANT SELECT ANY dictionary TO tdsys;

ALTER SESSION SET current_schema=tdsys;

-- install the installation package
@../plsql/specs/TD_INSTALL.pks
@../plsql/wrapped_bodies/TD_INSTALL.plb

SET termout off

-- build the system repository
EXEC tdsys.td_install.build_sys_repo( p_tablespace => '&tablespace' );

-- create the Evolve repository
EXEC tdsys.td_install.build_repo( p_owner => '&rep_user', p_tablespace => '&tablespace');
