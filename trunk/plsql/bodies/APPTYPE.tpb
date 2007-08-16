CREATE OR REPLACE TYPE BODY apptype
AS
   CONSTRUCTOR FUNCTION apptype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT td_inst.client_info
   )
      RETURN SELF AS RESULT
   AS
      l_results   NUMBER;
   BEGIN
      -- read in all the previous values
      read_prev_info;
      -- first we need to populate the module attribute, because it helps us determine parameter values
      td_inst.module(LOWER( CASE
			    WHEN p_module IS NULL
			    THEN $$plsql_unit
			    ELSE $$plsql_unit || '.' || p_module
			    END
			  ));
      -- we also set the action, which may be used one day to fine tune parameters
      td_inst.action := LOWER( p_action );
      -- read previous app_info settings
      -- populate attributes with new app_info settings
      td_inst.client_info := NVL( p_client_info,  );
      
      RETURN;
   END apptype;
   MEMBER PROCEDURE change_action( p_action VARCHAR2 )
   AS
   BEGIN
      td_inst.action(p_action);
      td_inst.register;
   END change_action;
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      td_inst.action := prev_action;
      td_inst.module := prev_module;
      td_inst.client_info := prev_client_info;
      td_inst.register;
   END clear_app_info;
   
   MEMBER PROCEDURE read_prev_info
   AS
   BEGIN
      -- read in the previous values of all instrumentation attributes
      prev_action := td_ext.action;
      prev_module := td_ext.module;
      prev_client_info := td_ext.client_info;
      prev_registration := td_ext.registration;
      prev_logging_level := td_ext.logging_level;
      prev_batch_id := td_ext.batch_id;
      prev_runmode := td_ext.runmode;

   END read_prev_info;
END;
/