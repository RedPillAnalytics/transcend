SET serveroutput on size unlimited
SET echo off

ACCEPT repo_user char default 'TDREP_USER' prompt 'Username to configure as Transcend repository user [TDREP_USER]':
ACCEPT repo_schema char default 'TDREP' prompt 'Schema containing Transcend repository for this user [TDREP]':
ACCEPT app_schema char default 'TDREP' prompt 'Schema containing Transcend repository for this user [TDREP]':

-- create synonyms from the user to the repository
@@rep_syns &repo_user &repo_schema

-- create synonyms from the user to the application
@@app_syns &repo_user &app_schema