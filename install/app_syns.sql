-- &1  is the synonym schema
-- &2 is the object schema

DECLARE
   l_syn_schema VARCHAR2(30) := '&1';
   l_obj_schema VARCHAR2(30) := '&2';
BEGIN   
   IF upper(l_syn_schema) <> upper(l_obj_schema)
   THEN
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.td_fileapi for '||l_obj_schema||'.td_fileapi';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.td_dbapi for '||l_obj_schema||'.td_dbapi';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.td_owbapi for '||l_obj_schema||'.td_owbapi';
   END IF;
      
END;
/
