SET echo off
SET verify off
PROMPT 'Running install.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallEvolve_&_DATE..log

-- get the CURRENT_SCHEMA
VARIABLE current_schema char(30)
EXEC :current_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- get the schema for the Evolve repository (tables)
ACCEPT rep_schema char default 'TDSYS' prompt 'Schema name for the Evolve default repository [tdsys]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDSYS' prompt 'Tablespace in which to install Evolve default repository: [tdsys]: '
-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDSYS' prompt 'Schema name for the Evolve application [tdsys]: '
-- find out whether destructive actions are okay
ACCEPT drop_obj char default 'N' prompt 'Do you want to issue DROP TABLE statements for any existing repository tables? [N]: '

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

DECLARE
   l_drop BOOLEAN := CASE WHEN REGEXP_LIKE('yes','&drop_obj','i') THEN TRUE ELSE FALSE END;
BEGIN
   -- build the system repository
   tdsys.td_install.build_sys_repo( p_schema=> 'tdsys', p_tablespace => '&tablespace', p_drop => l_drop );
   -- create the Evolve repository
   tdsys.td_install.build_evolve_repo( p_schema => '&rep_schema', p_tablespace => '&tablespace', p_drop => l_drop);
   -- create the Evolve application
   tdsys.td_install.build_evolve_app( p_schema => '&app_schema', p_repository => '&rep_schema', p_drop => l_drop);   
EXCEPTION
   WHEN tdsys.td_install.e_repo_obj_exists
   THEN
   raise_application_error(-20003,'Repository tables exist. Specify ''Y'' when prompted to issue DROP TABLE statements');
END;
/


-- now install the Evolve code
-- first drop the types
EXEC tdsys.td_install.drop_evolve_types;

--CREATE java stored procedure
@../java/TdCore.jvs

--CREATE core pieces needed by types
@../plsql/specs/SPLIT_OT.tps
@../plsql/specs/STRING_AGG_OT.tps
@../plsql/wrapped_bodies/STRING_AGG_OT.plb
@../plsql/wrapped_bodies/STRAGG.plb
@../plsql/specs/TD_CORE.pks
@../plsql/wrapped_bodies/TD_CORE.plb
@../plsql/specs/TD_INST.pks
@../plsql/wrapped_bodies/TD_INST.plb
@../plsql/specs/EVOLVE_LOG.pks
@../plsql/wrapped_bodies/EVOLVE_LOG.plb

-- crate the types
@../plsql/specs/APP_OT.tps
@../plsql/wrapped_bodies/APP_OT.plb
@../plsql/specs/NOTIFICATION_OT.tps
@../plsql/wrapped_bodies/NOTIFICATION_OT.plb
@../plsql/specs/EVOLVE_OT.tps
@../plsql/wrapped_bodies/EVOLVE_OT.plb

-- create the packages that use the types
@../plsql/specs/TD_UTILS.pks
@../plsql/wrapped_bodies/TD_UTILS.plb

-- create callable packages
@../plsql/specs/EVOLVE_APP.pks
@../plsql/wrapped_bodies/EVOLVE_APP.plb
@../plsql/specs/EVOLVE_ADM.pks
@../plsql/wrapped_bodies/EVOLVE_ADM.plb

-- grant execute on all the callable packages to the _APP role
EXEC tdsys.td_install.grant_evolve_app_privs( p_schema => '&app_schema' );

-- set the default logging, registration and runmodes
EXEC evolve_adm.set_default_configs;

SPOOL off

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:current_schema;
END;
/