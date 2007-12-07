CREATE OR REPLACE PACKAGE BODY trans_adm
IS
   PROCEDURE set_default_configs
   IS
   BEGIN
      -- load the entries into the ERROR_CONF table for Transcend
      set_error_conf( p_name=>'no_files_found',
		      p_code=>'No files found for this configuration');
      set_error_conf( p_name=>'no_ext_files',
		      p_code=>'There are no files found for this external table');
      set_error_conf( p_name=>'reject_limit_exceeded',
		      p_code=>'The external table reject limit was exceeded');
      set_error_conf( p_name=>'ext_file_missing',
		      p_code=>'The physical file for the specified external table does not exist');
      set_error_conf( p_name=>'on_clause_missing',
		      p_code=>'Either a unique constraint must exist on the target table, or a value for P_COLUMNS must be specified');
      set_error_conf( p_name=>'incorrect_parameters',
		      p_code=>'The combination of parameters provided yields no matching objects.');
      set_error_conf( p_name=>'file_too_big',
		      p_code=>'File size larger than MAX_BYTES paramter');
      set_error_conf( p_name=>'file_too_small',
		      p_code=>'File size smaller than MAX_BYTES paramter');
      set_error_conf( p_name=>'no_stats',
		      p_code=>'The specified segment has no stored statistics');
      set_error_conf( p_name=>'owb_flow_err',
		      p_code=>'An error was returned from the OWB Control Center');
      set_error_conf( p_name=>'data_cartridge',
		      p_code=>'An unregistered data cartridge error was returned while selecting from the specified external table');

   END reset_default_configs;

END trans_adm;
/

SHOW errors