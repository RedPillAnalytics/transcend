prompt 'Running rep_syns.sql'
SET show off
-- &1 IS the repository USER
DEFINE user_rs = &1
-- &2 IS the repository SCHEMA
DEFINE schema_rs = &2

-- create the user_rs user if it doesn't already exist
@create_app_user &user_rs

WHENEVER sqlerror exit sql.sqlcode
-- create synonyms for repository objects
DECLARE
   e_obj_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
BEGIN   
   IF upper('&user_rs') <> upper('&schema_rs')
   THEN
      -- tables
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..COUNT_TABLE for &schema_rs..COUNT_TABLE';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..DIR_LIST for &schema_rs..DIR_LIST';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..ERROR_CONF for &schema_rs..ERROR_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..FILES_CONF for &schema_rs..FILES_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..FILES_DETAIL for &schema_rs..FILES_DETAIL';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..FILES_OBJ_DETAIL for &schema_rs..FILES_OBJ_DETAIL';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..LOGGING_CONF for &schema_rs..LOGGING_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..LOG_TABLE for &schema_rs..LOG_TABLE';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..NOTIFICATION_CONF for &schema_rs..NOTIFICATION_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..NOTIFICATION_EVENTS for &schema_rs..NOTIFICATION_EVENTS';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..TD_PART_GTT for &schema_rs..TD_PART_GTT';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..TD_BUILD_IDX_GTT for &schema_rs..TD_BUILD_IDX_GTT';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..TD_BUILD_CON_GTT for &schema_rs..TD_BUILD_CON_GTT';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..TD_CON_MAINT_GTT for &schema_rs..TD_CON_MAINT_GTT';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..REGISTRATION_CONF for &schema_rs..REGISTRATION_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..RUNMODE_CONF for &schema_rs..RUNMODE_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..PARAMETER_CONF for &schema_rs..PARAMETER_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..COLUMN_CONF for &schema_rs..COLUMN_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..DIMENSION_CONF for &schema_rs..DIMENSION_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..COLUMN_TYPE_LIST for &schema_rs..COLUMN_TYPE_LIST';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..REPLACE_METHOD_LIST for &schema_rs..REPLACE_METHOD_LIST';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..OPT_STATS for &schema_rs..OPT_STATS';
      -- sequences
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..FILES_DETAIL_SEQ for &schema_rs..FILES_DETAIL_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &user_rs..FILES_OBJ_DETAIL_SEQ for &schema_rs..FILES_OBJ_DETAIL_SEQ';
   END IF;
EXCEPTION
WHEN e_obj_exists
   THEN
   raise_application_error(-20001, 'Schema contains repository objects. They should be removed before continuing.');
END;
/
WHENEVER sqlerror continue
