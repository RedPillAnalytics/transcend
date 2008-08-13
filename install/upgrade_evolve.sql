SET echo off
SET verify off
PROMPT 'Running upgrade_evolve.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL UpgradeEvolve_&_DATE..log

-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDREP' prompt 'Schema name for the application [tdrep]: '
-- get the schema for the Evolve repository (tables)
ACCEPT rep_schema char default 'TDREP' prompt 'Schema name for the default repository for this application [tdrep]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDREP' prompt 'Tablespace for the default repository: [tdrep]: '

WHENEVER sqlerror exit sql.sqlcode

-- upgrade the tdsys repository
@upgrade_tdsys_repo.sql

BEGIN
   -- upgrade the Evolve repository
   tdsys.td_adm.upgrade_evolve_repo( p_schema => '&rep_schema', p_tablespace => '&tablespace');
   -- prepare for the Evolve application
   tdsys.td_adm.build_evolve_app( p_schema => '&app_schema', p_repository => '&rep_schema');
EXCEPTION
   WHEN tdsys.td_adm.repo_obj_exists
   THEN
   raise_application_error(-20003,'Repository tables exist. Specify ''Y'' when prompted to issue DROP TABLE statements');
END;
/

-- this type is created first as it's needed for the TD_CORE
@../evolve/plsql/specs/SPLIT_OT.tps

-- create collection of libraries that make no use of the Evolve repository
-- these don't perform any real SQL at all
-- simply a series of reusable functions that don't have any external dependencies
@../evolve/plsql/specs/TD_CORE.pks

-- non-packaged functions because STRAGG cannot be packaged
@../evolve/plsql/specs/STRING_AGG_OT.tps
@../evolve/plsql/wrapped_bodies/STRAGG.plb

-- create java stored procedures
-- this contains OS and file level utilites that aren't available in other API's
@../evolve/java/TdCore.jvs

-- create Evolve pieces that don't use any repository objects
-- this in essence becomes "Evolve-lite" where no configuration or audit tables are required
@../evolve/plsql/specs/TD_INST.pks
@../evolve/plsql/specs/APP_OT.tps

-- layer in the utilities that require repository objects
-- this starts to move past "Evolve-lite"
@../evolve/plsql/specs/EVOLVE.pks
@../evolve/plsql/specs/NOTIFICATION_OT.tps
@../evolve/plsql/specs/EVOLVE_OT.tps

-- create utilities package that uses the main Evolve framework
@../evolve/plsql/specs/TD_UTILS.pks

-- create callable packages
@../evolve/plsql/specs/EVOLVE_ADM.pks

-- now compile all the package bodies
@../evolve/plsql/wrapped_bodies/STRING_AGG_OT.plb
@../evolve/plsql/wrapped_bodies/TD_CORE.plb
@../evolve/plsql/wrapped_bodies/TD_INST.plb
@../evolve/plsql/wrapped_bodies/APP_OT.plb
@../evolve/plsql/wrapped_bodies/EVOLVE.plb
@../evolve/plsql/wrapped_bodies/NOTIFICATION_OT.plb
@../evolve/plsql/wrapped_bodies/EVOLVE_OT.plb
@../evolve/plsql/wrapped_bodies/TD_UTILS.plb
@../evolve/plsql/wrapped_bodies/EVOLVE_ADM.plb

SPOOL off