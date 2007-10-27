PROMPT 'Running app_syns.sql'
-- &1  is the synonym SCHEMA
DEFINE syn_schema_as = &1
-- &2 is the object SCHEMA
DEFINE obj_schema_as = &2

BEGIN   
   IF upper('&syn_schema_as') <> upper('&obj_schema_as')
   THEN
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_ext for &obj_schema_as..td_ext';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_files for &obj_schema_as..td_files';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_host for &obj_schema_as..td_host';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_ddl for &obj_schema_as..td_ddl';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_etl for &obj_schema_as..td_etl';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_owbapi for &obj_schema_as..td_owbapi';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_control for &obj_schema_as..td_control';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..td_inst for &obj_schema_as..td_inst';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..stragg for &obj_schema_as..stragg';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..feed_ov for &obj_schema_as..feed_ov';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..extract_ov for &obj_schema_as..extract_ov';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schema_as..email_ov for &obj_schema_as..email_ov';
   END IF;
      
END;
/
