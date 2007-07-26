PROMPT 'Running app_syns.sql'
-- &1  is the synonym SCHEMA
DEFINE syn_schema = &1
-- &2 is the object SCHEMA
DEFINE obj_schema = &2

DECLARE
   l_syn_schema VARCHAR2(30) := '&syn_schema';
   l_obj_schema VARCHAR2(30) := '&obj_schema';
BEGIN   
   IF upper(l_syn_schema) <> upper(l_obj_schema)
   THEN
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.td_fileapi for '||l_obj_schema||'.td_fileapi';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.td_dbapi for '||l_obj_schema||'.td_dbapi';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.td_owbapi for '||l_obj_schema||'.td_owbapi';
   END IF;
      
END;
/
