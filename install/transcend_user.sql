PROMPT 'Running transcend_user.sql'
-- &1 IS the repository USER
DEFINE rep_user = &1
-- &2 IS the repository SCHEMA
DEFINE rep_schema = &2
-- &3 IS the application SCHEMA
DEFINE app_schema =&3

-- create synonyms from the user to the repository
@@rep_syns &rep_user &rep_schema

-- create synonyms from the user to the application
@@app_syns &rep_user &app_schema

-- need to give roles for the repository and the application
GRANT &rep_schema._adm TO &rep_user;
GRANT &app_schema._app TO &rep_user;