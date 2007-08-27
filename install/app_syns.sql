PROMPT 'Running app_syns.sql'
-- &1  is the synonym SCHEMA
DEFINE syn_schema_as = &1
-- &2 is the object SCHEMA
DEFINE obj_schema_as = &2

BEGIN   
   IF upper('&syn_schema_as') <> upper('&obj_schema_as')
   THEN
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_ext for &obj_schema_as..td_ext';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_fileapi for &obj_schema_as..td_fileapi';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_dbapi for &obj_schema_as..td_dbapi';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_owbapi for &obj_schema_as..td_owbapi';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_control for &obj_schema_as..td_control';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_inst for &obj_schema_as..td_inst';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..stragg for &obj_schema_as..stragg';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..feed_ot for &obj_schema_as..feed_ot';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..extract_ot for &obj_schema_as..extract_ot';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..email_ot for &obj_schema_as..email_ot';
   END IF;
      
END;
/
