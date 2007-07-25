-- create synonyms for repository objects
-- first parameter passed is the synonym schema
-- second parameter passed is the object schema

DECLARE
   l_syn_schema VARCHAR2(30) := '&1';
   l_obj_schema VARCHAR2(30) := '&2';
BEGIN   
   IF upper(l_syn_schema) <> upper(l_obj_schema)
   THEN
      -- tables
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.COUNT_TABLE for '||l_obj_schema||'.COUNT_TABLE';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.DIR_LIST for '||l_obj_schema||'.DIR_LIST';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.ERR_CD for '||l_obj_schema||'.ERR_CD';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.FILEHUB_CONF for '||l_obj_schema||'.FILEHUB_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.FILEHUB_DETAIL for '||l_obj_schema||'.FILEHUB_DETAIL';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.FILEHUB_OBJ_DETAIL for '||l_obj_schema||'.FILEHUB_OBJ_DETAIL';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.LOGGING_CONF for '||l_obj_schema||'.LOGGING_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.LOG_TABLE for '||l_obj_schema||'.LOG_TABLE';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.NOTIFY_CONF for '||l_obj_schema||'.NOTIFY_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.PARTNAME for '||l_obj_schema||'.PARTNAME';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.REGISTRATION_CONF for '||l_obj_schema||'.REGISTRATION_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.RUNMODE_CONF for '||l_obj_schema||'.RUNMODE_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.PARAMETER_CONF for '||l_obj_schema||'.PARAMETER_CONF';
      -- sequences
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.FILEHUB_CONF_SEQ for '||l_obj_schema||'.FILEHUB_CONF_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.FILEHUB_DETAIL_SEQ for '||l_obj_schema||'.FILEHUB_DETAIL_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.FILEHUB_OBJ_DETAIL_SEQ for '||l_obj_schema||'.FILEHUB_OBJ_DETAIL_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.LOGGING_CONF_SEQ for '||l_obj_schema||'.LOGGING_CONF_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.NOTIFY_CONF_SEQ for '||l_obj_schema||'.NOTIFY_CONF_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.REGISTRATION_CONF_SEQ for '||l_obj_schema||'.REGISTRATION_CONF_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.RUNMODE_CONF_SEQ for '||l_obj_schema||'.RUNMODE_CONF_SEQ';
      EXECUTE IMMEDIATE 'create synonym '||l_syn_schema||'.PARAMETER_CONF_SEQ for '||l_obj_schema||'.PARAMETER_CONF_SEQ';
   END IF;
      
END;
/
