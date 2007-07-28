PROMPT 'Running transcend_user.sql'
-- &1 IS the repository USER
DEFINE rep_user_tu = &1
-- &2 IS the repository SCHEMA
DEFINE rep_schema_tu = &2
-- &3 IS the application SCHEMA
DEFINE app_schema_tu =&3

-- create the user if it doesn't exist
@@create_app_user &rep_user_tu

-- create synonyms from the user to the repository
@@rep_syns &rep_user_tu &rep_schema_tu

-- create synonyms from the user to the application
@@app_syns &rep_user_tu &app_schema_tu

-- need to give roles for the repository and the application
GRANT &rep_schema_tu._adm TO &rep_user_tu;
GRANT &app_schema_tu._app TO &rep_user_tu;