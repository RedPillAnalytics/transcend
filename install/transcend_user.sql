PROMPT 'Running transcend_user.sql'
-- &1 IS the repository USER
-- &2 IS the repository SCHEMA
-- &3 IS the application SCHEMA

-- create synonyms from the user to the repository
@@rep_syns &1 &2

-- create synonyms from the user to the application
@@app_syns &1 &3

-- need to give roles for the repository and the application
GRANT &2._adm TO &1;
GRANT &3._app TO &1;