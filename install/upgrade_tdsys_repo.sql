SET echo off
SET verify off
PROMPT 'Running upgrade_tdsys_repo.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL UpgradeTdsys_&_DATE..log

-- get the CURRENT_SCHEMA
VARIABLE td_curr_schema char(30)
EXEC :td_curr_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- perform any upgrades to the tdsys repository
-- ticket:95
-- add version and product columns to the sys repository tables
alter table tdsys.repositories add product VARCHAR2(20);
alter table tdsys.applications add product VARCHAR2(20);
alter table tdsys.users add product VARCHAR2(20);

alter table tdsys.repositories add version NUMBER;
alter table tdsys.applications add version NUMBER;
alter table tdsys.users add version NUMBER;

-- the default version will be 1.2 for anything without a version number
-- currently no Evolve-only customers, so the default product will be 'transcend'
update tdsys.repositories set version='1.2', product='transcend' where version is null;
update tdsys.applications set version='1.2', product='transcend' where version is null;
update tdsys.users set version='1.2', product='transcend' where version is null;
      
-- install the installation package
@../plsql/specs/TD_ADM.pks
@../plsql/wrapped_bodies/TD_ADM.plb


BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:td_curr_schema;
END;
/

SPOOL off
