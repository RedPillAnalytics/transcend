PROMPT 'Running InstallTranscend.sql'
SET serveroutput on size unlimited
SET echo off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTranscend_&_DATE..log

-- first get the schema for the Transcend repository (tables) first
ACCEPT rep_schema_it char default 'TDREP' prompt 'Schema name for the Transcend repository [tdrep]: '
-- get the tablespace for the repository
ACCEPT tablespace_it char default 'TDREP' prompt 'Tablespace in which to install Transcend default repository: [tdrep]: '
-- get application user
ACCEPT app_schema_it char default 'TDREP' prompt 'Schema name for the Transcend application [tdrep]: '

-- install the repository
@@transcend_repository &rep_schema_it &tablespace_it

-- create the synonyms between the two if they are different
@@rep_syns &app_schema_it &rep_schema_it

-- install the Transcend application (stored code)
@@transcend_application &app_schema_it

-- set application defaults
@@rep_schema_defaults

-- set default tablespace back
-- it was only changed if the user already existed
BEGIN
   IF :tbspace_changed = 'yes'
   THEN
      EXECUTE IMMEDIATE 'alter user &rep_schema_it default tablespace '||:old_tbspace;
   END IF;
END;
/

SPOOL off;