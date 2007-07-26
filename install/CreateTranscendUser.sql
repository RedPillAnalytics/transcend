PROMPT 'Running CreateTranscendUser.sql'
SET serveroutput on size unlimited
SET echo off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL CreateTranscendUser_&_DATE..log

ACCEPT repo_user char default 'TDREP_USER' prompt 'Username to configure as Transcend repository user [tdrep_user]:'
ACCEPT repo_schema char default 'TDREP' prompt 'Schema containing Transcend repository for this user [tdrep]:'
ACCEPT app_schema char default 'TDREP' prompt 'Schema containing Transcend application for this user [tdrep]:'
-- lock the application schema if desired
ACCEPT response char default 'yes' prompt 'Do you want to lock the &app_schema schema? [yes]:'

-- call the modular script
@@transcend_user &repo_user &repo_schema &app_schema


DECLARE
   l_response VARCHAR2(30) := lower('&response');
   l_app_schema VARCHAR2(30) := upper('&app_schema');
BEGIN   
   IF l_response = 'yes'
   THEN
      EXECUTE IMMEDIATE 'alter user '||l_app_schema||' account lock';
   END IF;      
END;
/