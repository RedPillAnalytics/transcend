SET serveroutput on size unlimited
SET echo off

ACCEPT repo_user char default 'TDREP_USER' prompt 'Username to configure as Transcend repository user [tdrep_user]:'
ACCEPT repo_schema char default 'TDREP' prompt 'Schema containing Transcend repository for this user [tdrep]:'
ACCEPT app_schema char default 'TDREP' prompt 'Schema containing Transcend application for this user [tdrep]:'

-- create synonyms from the user to the repository
@@rep_syns &repo_user &repo_schema

-- create synonyms from the user to the application
@@app_syns &repo_user &app_schema

ACCEPT response char default 'yes' prompt 'Do you want to lock the &app_schema schema? [yes]:'

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