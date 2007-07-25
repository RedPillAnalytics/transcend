SET serveroutput on size unlimited
SET echo off

ACCEPT repo_user char default 'TDREP_USER' prompt 'Username to configure as Transcend repository user [tdrep_user]:'
ACCEPT repo_schema char default 'TDREP' prompt 'Schema containing Transcend repository for this user [tdrep]:'
ACCEPT app_schema char default 'TDREP' prompt 'Schema containing Transcend application for this user [tdrep]:'

-- create synonyms from the user to the repository
@@rep_syns &repo_user &repo_schema

-- create synonyms from the user to the application
@@app_syns &repo_user &app_schema

-- need to give roles for the repository and the application
GRANT &repo_schema._adm TO &repo_user;
GRANT &app_schema._app TO &repo_user;