CREATE OR REPLACE PACKAGE BODY trans_adm
IS
   PROCEDURE set_default_configs
   IS
   BEGIN

      -- set the notification events      
      evolve_adm.set_notification_event('audit_file','file too large','File outside size threshholds','The file referenced below is larger than the configured threshhold:');
      evolve_adm.set_notification_event('audit_file','file too small','File outside size threshholds','The file referenced below is smaller than the configured threshhold:');

      -- load the entries into the ERROR_CONF table for Transcend
      evolve_adm.set_error_conf( p_name=>'no_files_found',
				 p_message=>'No files found for this configuration');
      evolve_adm.set_error_conf( p_name=>'no_ext_files',
				 p_message=>'There are no files found for this external table');
      evolve_adm.set_error_conf( p_name=>'reject_limit_exceeded',
				 p_message=>'The external table reject limit was exceeded');
      evolve_adm.set_error_conf( p_name=>'ext_file_missing',
				 p_message=>'The physical file for the specified external table does not exist');
      evolve_adm.set_error_conf( p_name=>'on_clause_missing',
				 p_message=>'Either a unique constraint must exist on the target table, or a value for P_COLUMNS must be specified');
      evolve_adm.set_error_conf( p_name=>'incorrect_parameters',
				 p_message=>'The combination of parameters provided yields no matching objects.');
      evolve_adm.set_error_conf( p_name=>'file_too_big',
				 p_message=>'File size larger than MAX_BYTES paramter');
      evolve_adm.set_error_conf( p_name=>'file_too_small',
				 p_message=>'File size smaller than MAX_BYTES paramter');
      evolve_adm.set_error_conf( p_name=>'no_stats',
				 p_message=>'The specified segment has no stored statistics');
      evolve_adm.set_error_conf( p_name=>'owb_flow_err',
				 p_message=>'An error was returned from the OWB Control Center');
      evolve_adm.set_error_conf( p_name=>'data_cartridge',
				 p_message=>'An unregistered data cartridge error was returned while selecting from the specified external table');
      evolve_adm.set_error_conf( p_name=>'submit_sql',
				 p_message=>'An error was generated by a process submitted to the Oracle scheduler. See the scheduler logs for details.');

   END set_default_configs;

END trans_adm;
/

SHOW errors