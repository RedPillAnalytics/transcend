PROMPT 'Running transcend_user.sql'
-- &1 IS the repository USER
-- &2 IS the repository SCHEMA
-- &3 IS the application SCHEMA

DEFINE rep_user = &1
DEFINE rep_schema = &2
DEFINE app_schema =&3
-- create synonyms from the user to the repository
@@rep_syns &rep_user $rep_schema

-- create synonyms from the user to the application
@@app_syns &rep_user &app_schemav

-- need to give roles for the repository and the application
GRANT &2._adm TO &1;
GRANT &3._app TO &1;