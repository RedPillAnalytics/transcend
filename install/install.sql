SET serveroutput on size unlimited
SET echo off

-- install the Transcend repository (tables) first
ACCEPT td_rep char default 'TD_REP' prompt 'Schema name for the Transcend repository [TD_REP]: '
@transcend_repository &td_rep
ACCEPT td_app char default 'TD_REP' prompt 'Schema name for the Transcend application [TD_REP]: '
DECLARE
   l_rep_schema VARCHAR2(30) := '&rep_schema';
   l_app_schema VARCHAR2(30) := '&app_schema';
BEGIN   
   IF upper(l_rep_schema) <> upper(l_app_schema)
   THEN
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.COUNT_TABLE for '||l_rep_schema||'.COUNT_TABLE';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.DIR_LIST for '||l_rep_schema||'.DIR_LIST';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.ERR_CD for '||l_rep_schema||'.ERR_CD';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.FILEHUB_CONF for '||l_rep_schema||'.FILEHUB_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.FILEHUB_DETAIL for '||l_rep_schema||'.FILEHUB_DETAIL';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.FILEHUB_OBJ_DETAIL for '||l_rep_schema||'.FILEHUB_OBJ_DETAIL';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.LOGGING_CONF for '||l_rep_schema||'.LOGGING_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.LOG_TABLE for '||l_rep_schema||'.LOG_TABLE';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.NOTIFY_CONF for '||l_rep_schema||'.NOTIFY_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.PARTNAME for '||l_rep_schema||'.PARTNAME';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.REGISTRATION_CONF for '||l_rep_schema||'.REGISTRATION_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.RUNMODE_CONF for '||l_rep_schema||'.RUNMODE_CONF';
      EXECUTE IMMEDIATE 'create synonym '||l_app_schema||'.PARAMETER_CONF for '||l_rep_schema||'.PARAMETER_CONF';
   END IF;
      
END;
/
-- install the Transcend application (stored code)
@transcend_application &td_app

ALTER SESSION SET current_schema=&_USER;

BEGIN
   EXECUTE IMMEDIATE 'alter user &tab_schema default tablespace '||:old_tbspace;
END;
/