prompt 'Running rep_syns.sql'

-- &1 IS the repository USER
DEFINE rep_user_rs = &1
-- &2 IS the repository SCHEMA
DEFINE rep_schema_rs = &2


-- create synonyms for repository objects
-- first parameter passed is the synonym schema
-- second parameter passed is the object schema

BEGIN   
   IF upper('&rep_user_rs') <> upper('&rep_schema_rs')
   THEN
      -- tables
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..COUNT_TABLE for &rep_schema_rs..COUNT_TABLE';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..DIR_LIST for &rep_schema_rs..DIR_LIST';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..ERR_CD for &rep_schema_rs..ERR_CD';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..FILEHUB_CONF for &rep_schema_rs..FILEHUB_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..FILEHUB_DETAIL for &rep_schema_rs..FILEHUB_DETAIL';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..FILEHUB_OBJ_DETAIL for &rep_schema_rs..FILEHUB_OBJ_DETAIL';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..LOGGING_CONF for &rep_schema_rs..LOGGING_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..LOG_TABLE for &rep_schema_rs..LOG_TABLE';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..NOTIFY_CONF for &rep_schema_rs..NOTIFY_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..PARTNAME for &rep_schema_rs..PARTNAME';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..REGISTRATION_CONF for &rep_schema_rs..REGISTRATION_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..RUNMODE_CONF for &rep_schema_rs..RUNMODE_CONF';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..PARAMETER_CONF for &rep_schema_rs..PARAMETER_CONF';
      -- sequences
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..FILEHUB_CONF_SEQ for &rep_schema_rs..FILEHUB_CONF_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..FILEHUB_DETAIL_SEQ for &rep_schema_rs..FILEHUB_DETAIL_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..FILEHUB_OBJ_DETAIL_SEQ for &rep_schema_rs..FILEHUB_OBJ_DETAIL_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..LOGGING_CONF_SEQ for &rep_schema_rs..LOGGING_CONF_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..NOTIFY_CONF_SEQ for &rep_schema_rs..NOTIFY_CONF_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..REGISTRATION_CONF_SEQ for &rep_schema_rs..REGISTRATION_CONF_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..RUNMODE_CONF_SEQ for &rep_schema_rs..RUNMODE_CONF_SEQ';
      EXECUTE IMMEDIATE 'create or replace synonym &rep_user_rs..PARAMETER_CONF_SEQ for &rep_schema_rs..PARAMETER_CONF_SEQ';
   END IF;
END;
/
