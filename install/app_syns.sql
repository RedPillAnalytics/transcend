PROMPT 'Running app_syns.sql'
-- &1  is the synonym SCHEMA
DEFINE syn_schema_as = &1
-- &2 is the object SCHEMA
DEFINE obj_schema_as = &2

BEGIN   
   IF upper('&syn_schema_as') <> upper('&obj_schema_as')
   THEN
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schem..td_fileapi for &obj_schem..td_fileapi';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schem..td_dbapi for &obj_schem..td_dbapi';
      EXECUTE IMMEDIATE 'create or replace synonym &syn_schem..td_owbapi for &obj_schem..td_owbapi';
   END IF;
      
END;
/
