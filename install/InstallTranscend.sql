PROMPT 'Running InstallTranscend.sql'
SET serveroutput on size unlimited
SET echo off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTranscend_&_DATE..log

-- first get the schema for the Transcend repository (tables) first
ACCEPT it_rep_schema char default 'TDREP' prompt 'Schema name for the Transcend repository [tdrep]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDREP' prompt 'Tablespace in which to install Transcend default repository: [tdrep]: '
-- get application user
ACCEPT it_app_schema char default 'TDREP' prompt 'Schema name for the Transcend application [tdrep]: '

-- install the repository
@@transcend_repository &it_rep_schema &it_tablespace

-- create the synonyms between the two if they are different
@@rep_syns &it_app_schema &it_rep_schema

-- install the Transcend application (stored code)
@@transcend_application &it_app_schema

-- set application defaults
@@it_rep_schema_defaults

-- set default tablespace back
-- it was only changed if the user already existed
BEGIN
   IF :tbspace_changed = 'yes'
   THEN
      EXECUTE IMMEDIATE 'alter user &it_rep_schema default tablespace '||:old_tbspace;
   END IF;
END;
/

SPOOL off;