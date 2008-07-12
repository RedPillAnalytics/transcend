--SET echo off
SET verify off
PROMPT 'Running install_evolve.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallEvolve_&_DATE..log

-- get the CURRENT_SCHEMA
VARIABLE current_schema char(30)
EXEC :current_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- get the schema for the Evolve repository (tables)
ACCEPT rep_schema char default 'TDSYS' prompt 'Schema name for the default repository [tdsys]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDSYS' prompt 'Tablespace in which to install default repository: [tdsys]: '
-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDSYS' prompt 'Schema name for the application [tdsys]: '
-- find out whether destructive actions are okay
ACCEPT drop_obj char default 'N' prompt 'Do you want to issue DROP TABLE statements for any existing repository tables? [N]: '

WHENEVER sqlerror exit sql.sqlcode

DECLARE
   l_user           all_users.username%TYPE;
   e_user_exists    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
   -- find out if the user exists
   -- also get the current default tablespace of the user
BEGIN
   SELECT username
     INTO l_user
     FROM dba_users
    WHERE username = 'TDSYS';

EXCEPTION
   -- the user does not exist
   WHEN NO_DATA_FOUND
   THEN
      EXECUTE IMMEDIATE 'CREATE USER tdsys identified by no2tdsys default tablespace &tablespace quota unlimited on &tablespace';
END;
/

-- needed to interact with users and their tablespaces
GRANT SELECT ANY dictionary TO tdsys;

ALTER SESSION SET current_schema=tdsys;

-- install the installation package
@../plsql/specs/TD_ADM.pks
@../plsql/wrapped_bodies/TD_ADM.plb

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:current_schema;
END;
/

DECLARE
   l_drop BOOLEAN := CASE WHEN REGEXP_LIKE('yes','&drop_obj','i') THEN TRUE ELSE FALSE END;
BEGIN
   -- build the system repository
   tdsys.td_adm.build_sys_repo( p_schema=> 'tdsys', p_tablespace => '&tablespace', p_drop => l_drop );
   -- create the Evolve repository
   tdsys.td_adm.build_evolve_repo( p_schema => '&rep_schema', p_tablespace => '&tablespace', p_drop => l_drop);
   -- create the Evolve application
   tdsys.td_adm.build_evolve_app( p_schema => '&app_schema', p_repository => '&rep_schema', p_drop => l_drop);   
EXCEPTION
   WHEN tdsys.td_adm.e_repo_obj_exists
   THEN
   raise_application_error(-20003,'Repository tables exist. Specify ''Y'' when prompted to issue DROP TABLE statements');
END;
/


-- now install the Evolve code
-- first drop the types
EXEC tdsys.td_adm.drop_evolve_types;


-- this type is created first as it's needed for the TD_CORE
@../evolve/plsql/specs/SPLIT_OT.tps

-- create collection of libraries that make no use of the Evolve repository
-- these don't perform any real SQL at all
-- simply a series of reusable functions that don't have any external dependencies
@../evolve/plsql/specs/TD_CORE.pks
@../evolve/plsql/wrapped_bodies/TD_CORE.plb

-- non-packaged functions because STRAGG cannot be packaged
@../evolve/plsql/specs/STRING_AGG_OT.tps
@../evolve/plsql/wrapped_bodies/STRING_AGG_OT.plb
@../evolve/plsql/wrapped_bodies/STRAGG.plb

-- create java stored procedures
-- this contains OS and file level utilites that aren't available in other API's
@../evolve/java/TdCore.jvs

-- create Evolve pieces that don't use any repository objects
-- this in essence becomes "Evolve-lite" where no configuration or audit tables are required
@../evolve/plsql/specs/TD_INST.pks
@../evolve/plsql/wrapped_bodies/TD_INST.plb
@../evolve/plsql/specs/APP_OT.tps
@../evolve/plsql/wrapped_bodies/APP_OT.plb

-- layer in the utilities that require repository objects
-- this starts to move past "Evolve-lite"
@../evolve/plsql/specs/EVOLVE_LOG.pks
@../evolve/plsql/wrapped_bodies/EVOLVE_LOG.plb
@../evolve/plsql/specs/NOTIFICATION_OT.tps
@../evolve/plsql/wrapped_bodies/NOTIFICATION_OT.plb
@../evolve/plsql/specs/EVOLVE_OT.tps
@../evolve/plsql/wrapped_bodies/EVOLVE_OT.plb

-- create utilities package that uses the main Evolve framework
@../evolve/plsql/specs/TD_UTILS.pks
@../evolve/plsql/wrapped_bodies/TD_UTILS.plb

-- create callable packages
@../evolve/plsql/specs/EVOLVE_ADM.pks
@../evolve/plsql/wrapped_bodies/EVOLVE_ADM.plb

-- set the default logging, registration and runmodes
EXEC evolve_adm.set_default_configs;

SPOOL off

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:current_schema;
END;
/