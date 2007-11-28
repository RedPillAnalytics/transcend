PROMPT 'Running install.sql'
SET serveroutput on size unlimited
SET echo off
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTranscend_&_DATE..log

-- get the CURRENT_SCHEMA
VARIABLE current_schema char(30)
EXEC :current_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- get the schema for the Evolve repository (tables)
ACCEPT rep_schema char default 'TDSYS' prompt 'Schema name for the Evolve default repository [tdsys]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDSYS' prompt 'Tablespace in which to install Transcend default repository: [tdsys]: '
-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDSYS' prompt 'Schema name for the Transcend application [tdsys]: '

WHENEVER sqlerror exit sql.sqlcode

DECLARE
   e_user_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
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
@../../plsql/specs/TD_INSTALL.pks
@../../plsql/wrapped_bodies/TD_INSTALL.plb

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:current_schema;
END;
/

-- build the system repository
EXEC tdsys.td_install.build_sys_repo( p_schema=> 'tdsys', p_tablespace => '&tablespace' );

-- create the Evolve repository
EXEC tdsys.td_install.build_evolve_repo( p_schema => '&rep_schema', p_tablespace => '&tablespace');

-- create the Evolve application
EXEC tdsys.td_install.build_evolve_repo( p_schema => '&app_schema', p_repository => '&rep_schema');
