CREATE OR REPLACE PACKAGE BODY trans_adm
IS
   PROCEDURE set_default_configs
   IS
   BEGIN
      -- set the notification events
      evolve_adm.set_notification_event
         ('file_detail_ot.inspect',
          'file too large',
          'File outside size threshholds',
          'The file referenced below is larger than the configured threshhold:'
         );
      evolve_adm.set_notification_event
         ('file_detail_ot.inspect',
          'file too small',
          'File outside size threshholds',
          'The file referenced below is smaller than the configured threshhold:'
         );
      evolve_adm.set_notification_event
         ('file_label_ot.audit_file',
          'reject limit',
          'Too many rejected rows',
          'The file referenced below has too many rejected rows:'
         );
      -- load the entries into the ERROR_CONF table for Transcend
      evolve_adm.set_error_conf
                         (p_name         => 'no_files_found',
                          p_message      => 'No files found for this configuration'
                         );
      evolve_adm.set_error_conf
              (p_name         => 'no_ext_files',
               p_message      => 'There are no files found for this external table'
              );
      evolve_adm.set_error_conf
                  (p_name         => 'reject_limit_exceeded',
                   p_message      => 'The external table reject limit was exceeded'
                  );
      evolve_adm.set_error_conf
         (p_name         => 'ext_file_missing',
          p_message      => 'The physical file for the specified external table does not exist'
         );
      evolve_adm.set_error_conf
         (p_name         => 'fail_source_policy',
          p_message      => 'Multiple matching files found with a SOURCE_POLICY value of "fail"'
         );
      evolve_adm.set_error_conf
         (p_name         => 'on_clause_missing',
          p_message      => 'Either a unique constraint must exist on the target table, or a value for P_COLUMNS must be specified'
         );
      evolve_adm.set_error_conf
         (p_name         => 'notify_err',
          p_message      => 'There is an error with configuration for the specified notification'
         );
      evolve_adm.set_error_conf
         (p_name         => 'incorrect_parameters',
          p_message      => 'The combination of parameters provided yields no matching objects'
         );
      evolve_adm.set_error_conf
         (p_name         => 'file_too_large',
          p_message      => 'The specified file is larger than the MAX_BYTES parameter'
         );
      evolve_adm.set_error_conf
         (p_name         => 'file_too_small',
          p_message      => 'The specified file is smaller than the MAX_BYTES parameter'
         );
      evolve_adm.set_error_conf
         (p_name         => 'single_lob',
           p_message      => 'The AUDIT_FILE procedure requires either a single CLOB or a single BLOB'
         );
      evolve_adm.set_error_conf
                (p_name         => 'no_stats',
                 p_message      => 'The specified segment has no stored statistics'
                );
      evolve_adm.set_error_conf
         (p_name         => 'data_cartridge',
          p_message      => 'An unregistered data cartridge error was returned while selecting from the specified external table'
         );
      evolve_adm.set_error_conf
         (p_name         => 'multi_loc_ext_tab',
          p_message      => 'External tables used in Transcend Files must contain a single location'
         );
      evolve_adm.set_error_conf
         (p_name         => 'work_dir_name',
           p_message      => 'The values provided for DIRECTORY and WORK_DIRECTORY cannot be the same'
         );
      evolve_adm.set_error_conf
         (p_name         => 'work_dir_fs',
           p_message      => 'The directories configured for DIRECTORY and WORK_DIRECTORY cannot be on the same filesystem%'
         );
      evolve_adm.set_error_conf
                   (p_name         => 'no_ext_tab',
                    p_message      => 'The specified external table does not exist'
                   );
      evolve_adm.set_error_conf
               (p_name         => 'parms_combo',
                p_message      => 'The specified parameters are mutually inclusive'
               );
      evolve_adm.set_error_conf
         (p_name         => 'no_dim',
          p_message      => 'The specified table is not a configured dimension table'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_feed',
          p_message      => 'The specified feed has not been configured'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_extract',
          p_message      => 'The specified extract has not been configured'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_file_detail',
          p_message      => 'The specified file_detail record does not exist'
         );
      evolve_adm.set_error_conf
                 (p_name         => 'no_mapping',
                  p_message      => 'The specified mapping has not been configured'
                 );
      evolve_adm.set_error_conf
         (p_name         => 'dim_map_conf',
          p_message      => 'The mapping you are trying to configure is a dimensional load mapping. Use the procedure CONFIGURE_DIM to modify this configuration.'
         );
      evolve_adm.set_error_conf
         (p_name         => 'dim_mismatch',
          p_message      => 'There is a mismatch between columns in the source object and dimension table for the specified dimension table'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_curr_ind',
           p_message      => 'No current indicator attribute has been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'multiple_curr_ind',
           p_message      => 'Multiple current indicator attributes have been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_exp_dt',
           p_message      => 'No expiration date attribute has been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'multiple_exp_dt',
          p_message      => 'Multiple expiration date atributes have been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_eff_dt',
           p_message      => 'No effective date attribute has been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'multiple_eff_dt',
          p_message      => 'Multiple effective date atributes have been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_nat_key',
           p_message      => 'No natural key attribute has been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'no_surr_key',
           p_message      => 'No surrogate key attribute has been configured for the dimension'
         );
      evolve_adm.set_error_conf
         (p_name         => 'multiple_surr_key',
           p_message      => 'Multiple surrogate key atributes have been configured for the dimension'
         );
   END set_default_configs;

   PROCEDURE create_feed (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_directory	   VARCHAR2,
      p_source_directory   VARCHAR2,
      p_source_regexp      VARCHAR2,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_match_parameter    VARCHAR2 DEFAULT 'i',
      p_source_policy      VARCHAR2 DEFAULT 'newest',
      p_store_original     VARCHAR2 DEFAULT 'no',
      p_compress_method	   VARCHAR2 DEFAULT NULL,
      p_encrypt_method	   VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT 'yes',
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_reject_limit       NUMBER   DEFAULT 100,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT 'yes',
      p_description        VARCHAR2 DEFAULT NULL
   )
   IS
      e_dup_conf    EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_feed        feed_ot;
      o_ev          evolve_ot     := evolve_ot (p_module      => 'create_feed');
   BEGIN

      BEGIN
         INSERT INTO file_conf
		( file_label,
		  file_group,
		  label_type,
		  directory,
		  source_directory,
		  source_regexp,
		  work_directory,
		  object_owner,
		  object_name,
		  filename,
		  match_parameter,
		  source_policy,
		  store_original_files,
		  compress_method,
		  encrypt_method,
		  passphrase,
		  required,
		  min_bytes,
		  max_bytes,
		  reject_limit,
		  baseurl,
		  delete_source,
		  description
		)
	 VALUES ( p_file_label,
		  p_file_group,
		  'feed',
		  p_directory,
		  p_source_directory,
		  p_source_regexp,
		  p_work_directory,
		  p_owner,
		  p_table,
		  p_filename,
		  p_match_parameter,
		  p_source_policy,
		  p_store_original,
		  p_compress_method,
		  p_encrypt_method,
		  p_passphrase,
		  p_required,
		  p_min_bytes,
		  p_max_bytes,
		  p_reject_limit,
		  p_baseurl,
		  p_delete_source,
		  p_description
                );
      EXCEPTION
         WHEN e_dup_conf
         THEN
            evolve.raise_err ('dup_conf');
      END;
      
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4 );
      
      -- instantiate the feed_ot type so that the verify method is executed
      -- this method contains all the business logic to see if parameters are valid
      o_feed := feed_ot ( p_file_label      => p_file_label );
      o_ev.clear_app_info;
   END create_feed;

   PROCEDURE modify_feed (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2 DEFAULT NULL,
      p_directory	   VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_match_parameter    VARCHAR2 DEFAULT NULL,
      p_source_policy      VARCHAR2 DEFAULT NULL,
      p_store_original     VARCHAR2 DEFAULT NULL,
      p_compress_method	   VARCHAR2 DEFAULT NULL,
      p_encrypt_method	   VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_reject_limit       NUMBER   DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
   )
   IS
      e_dup_conf    EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_feed        feed_ot;
      o_ev          evolve_ot     := evolve_ot (p_module      => 'modify_feed');
   BEGIN
      
      -- if the constant NULL_VALUE is used, then the value should be set to null
      UPDATE file_conf
         SET file_label =
             CASE
             WHEN p_file_label IS NULL
             THEN file_label
             WHEN p_file_label = null_value
             THEN NULL
             ELSE p_file_label
             END,
	     file_group =
             CASE
             WHEN p_file_group IS NULL
             THEN file_group
             WHEN p_file_group = null_value
             THEN NULL
             ELSE p_file_group
             END,
	     directory =
             CASE
             WHEN p_directory IS NULL
             THEN directory
             WHEN p_directory = null_value
             THEN NULL
             ELSE p_directory
             END,
	     source_directory =
             CASE
             WHEN p_source_directory IS NULL
             THEN source_directory
             WHEN p_source_directory = null_value
             THEN NULL
             ELSE p_source_directory
             END,
	     source_regexp =
             CASE
             WHEN p_source_regexp IS NULL
             THEN source_regexp
             WHEN p_source_regexp = null_value
             THEN NULL
             ELSE p_source_regexp
             END,
	     work_directory =
             CASE
             WHEN p_work_directory IS NULL
             THEN work_directory
             WHEN p_work_directory = null_value
             THEN NULL
             ELSE p_work_directory
             END,
	     object_owner =
             CASE
             WHEN p_owner IS NULL
             THEN object_owner
             WHEN p_owner = null_value
             THEN NULL
             ELSE p_owner
             END,
	     object_name =
             CASE
             WHEN p_table IS NULL
             THEN object_name
             WHEN p_table = null_value
             THEN NULL
             ELSE p_table
             END,
	     filename =
             CASE
             WHEN p_filename IS NULL
             THEN filename
             WHEN p_filename = null_value
             THEN NULL
             ELSE p_filename
             END,
             match_parameter =
             CASE
             WHEN p_match_parameter IS NULL
             THEN match_parameter
             WHEN p_match_parameter = null_value
             THEN NULL
             ELSE p_match_parameter
             END,
             source_policy =
             CASE
             WHEN p_source_policy IS NULL
             THEN source_policy
             WHEN p_source_policy = null_value
             THEN NULL
             ELSE p_source_policy
             END,
             store_original_files =
             CASE
             WHEN p_store_original IS NULL
             THEN store_original_files
             WHEN p_store_original = null_value
             THEN NULL
             ELSE p_store_original
             END,
             compress_method =
             CASE
             WHEN p_compress_method IS NULL
             THEN compress_method
             WHEN p_compress_method = null_value
             THEN NULL
             ELSE p_compress_method
             END,
             encrypt_method =
             CASE
             WHEN p_encrypt_method IS NULL
             THEN encrypt_method
             WHEN p_encrypt_method = null_value
             THEN NULL
             ELSE p_encrypt_method
             END,
             passphrase =
             CASE
             WHEN p_passphrase IS NULL
             THEN passphrase
             WHEN p_passphrase = null_value
             THEN NULL
             ELSE p_passphrase
             END,
             required =
             CASE
             WHEN p_required IS NULL
             THEN required
             WHEN p_required = null_value
             THEN NULL
             ELSE p_required
             END,
             min_bytes =
             CASE
             WHEN p_min_bytes IS NULL
             THEN min_bytes
             WHEN p_min_bytes = null_value
             THEN NULL
             ELSE p_min_bytes
             END,
             max_bytes =
             CASE
             WHEN p_max_bytes IS NULL
             THEN max_bytes
             WHEN p_max_bytes = null_value
             THEN NULL
             ELSE p_max_bytes
             END,
             reject_limit =
             CASE
             WHEN p_reject_limit IS NULL
             THEN reject_limit
             WHEN p_reject_limit = null_value
             THEN NULL
             ELSE p_reject_limit
             END,
             baseurl =
             CASE
             WHEN p_baseurl IS NULL
             THEN baseurl
             WHEN p_baseurl = null_value
             THEN NULL
             ELSE p_baseurl
             END,
             delete_source =
             CASE
             WHEN p_delete_source IS NULL
             THEN delete_source
             WHEN p_delete_source = null_value
             THEN NULL
             ELSE p_delete_source
             END,
             description =
             CASE
             WHEN p_description IS NULL
             THEN description
             WHEN p_description = null_value
             THEN NULL
             ELSE p_description
             END,
	     modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE file_label = LOWER (p_file_label);
	     
       evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4 );

       o_feed := feed_ot ( p_file_label      => p_file_label );
       o_ev.clear_app_info;
   END modify_feed;

   PROCEDURE delete_feed (
      p_file_label         VARCHAR2
   )
   IS
      o_ev          evolve_ot     := evolve_ot (p_module      => 'delete_feed');
   BEGIN

      DELETE FROM file_conf
       WHERE file_label = LOWER (p_file_label);
      
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4 );
      o_ev.clear_app_info;

   END delete_feed;
      
   PROCEDURE create_extract (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_filename           VARCHAR2,
      p_object_owner       VARCHAR2,
      p_object_name        VARCHAR2,
      p_directory          VARCHAR2,
      p_work_directory     VARCHAR2,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss am',
      p_tsformat           VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
      p_delimiter          VARCHAR2 DEFAULT ',',
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT 'yes',
      p_min_bytes          NUMBER   DEFAULT 0,
      p_max_bytes          NUMBER   DEFAULT 0,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_reject_limit	   NUMBER   DEFAULT 0,
      p_description        VARCHAR2 DEFAULT NULL
   )
   IS
      o_extract    extract_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_ev         evolve_ot   := evolve_ot (p_module      => 'create_extract');
   BEGIN
      BEGIN
	 INSERT INTO file_conf
		( file_label, 
                  file_group, 
                  label_type, 
                  object_owner, 
                  object_name, 
                  directory, 
                  filename, 
                  work_directory, 
                  min_bytes, 
                  max_bytes, 
                  reject_limit, 
                  baseurl, 
                  file_datestamp, 
                  dateformat, 
                  timestampformat, 
                  delimiter, 
                  quotechar, 
                  headers
		)
	 VALUES ( p_file_label,
		  p_file_group, 
		  'extract', 
		  UPPER( p_object_owner ), 
		  UPPER( p_object_name ), 
		  UPPER( p_directory ), 
		  p_filename, 
		  UPPER( p_work_directory ),
		  p_min_bytes, 
		  p_max_bytes, 
		  p_reject_limit, 
		  p_baseurl, 
		  p_file_datestamp, 
		  p_dateformat, 
		  p_tsformat, 
		  p_delimiter, 
		  p_quotechar, 
		  p_headers
		);
      EXCEPTION
         WHEN e_dup_conf
         THEN
         evolve.raise_err ('dup_conf');
      END;

      -- instantiate an object to test it      
      o_extract := extract_ot ( p_file_label      => p_file_label );
      o_ev.clear_app_info;

   END create_extract;
   
   PROCEDURE modify_extract (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT NULL,
      p_tsformat           VARCHAR2 DEFAULT NULL,
      p_delimiter          VARCHAR2 DEFAULT NULL,
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_reject_limit	   NUMBER   DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
   )
   IS
      o_extract    extract_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_ev         evolve_ot   := evolve_ot (p_module      => 'modify_extract');
   BEGIN
         UPDATE file_conf
            SET file_group =
                   CASE
                      WHEN p_file_group IS NULL
                         THEN file_group
                      WHEN p_file_group = null_value
                         THEN NULL
                      ELSE p_file_group
                   END,
                filename =
                   CASE
                      WHEN p_filename IS NULL
                         THEN filename
                      WHEN p_filename = null_value
                         THEN NULL
                      ELSE p_filename
                   END,
                object_owner =
                   CASE
                      WHEN p_object_owner IS NULL
                         THEN object_owner
                      WHEN p_object_owner = null_value
                         THEN NULL
                      ELSE p_object_owner
                   END,
                object_name =
                   CASE
                      WHEN p_object_name IS NULL
                         THEN object_name
                      WHEN p_object_name = null_value
                         THEN NULL
                      ELSE p_object_name
                   END,
                directory =
                   CASE
                      WHEN p_directory IS NULL
                         THEN directory
                      WHEN p_directory = null_value
                         THEN NULL
                      ELSE p_directory
                   END,
                work_directory =
                   CASE
                      WHEN p_work_directory IS NULL
                         THEN work_directory
                      WHEN p_work_directory = null_value
                         THEN NULL
                      ELSE p_work_directory
                   END,
                file_datestamp =
                   CASE
                      WHEN p_file_datestamp IS NULL
                         THEN file_datestamp
                      WHEN p_file_datestamp = null_value
                         THEN NULL
                      ELSE p_file_datestamp
                   END,
                dateformat =
                   CASE
                      WHEN p_dateformat IS NULL
                         THEN dateformat
                      WHEN p_dateformat = null_value
                         THEN NULL
                      ELSE p_dateformat
                   END,
                timestampformat =
                   CASE
                      WHEN p_tsformat IS NULL
                         THEN timestampformat
                      WHEN p_tsformat = null_value
                         THEN NULL
                      ELSE p_tsformat
                   END,
                delimiter =
                   CASE
                      WHEN p_delimiter IS NULL
                         THEN delimiter
                      WHEN p_delimiter = null_value
                         THEN NULL
                      ELSE p_delimiter
                   END,
                quotechar =
                   CASE
                      WHEN p_quotechar IS NULL
                         THEN quotechar
                      WHEN p_quotechar = null_value
                         THEN NULL
                      ELSE p_quotechar
                   END,
                headers =
                   CASE
                      WHEN p_headers IS NULL
                         THEN headers
                      WHEN p_headers = null_value
                         THEN NULL
                      ELSE p_headers
                   END,
                min_bytes =
                   CASE
                      WHEN p_min_bytes IS NULL
                         THEN min_bytes
                      WHEN p_min_bytes = null_value
                         THEN NULL
                      ELSE p_min_bytes
                   END,
                max_bytes =
                   CASE
                      WHEN p_max_bytes IS NULL
                         THEN max_bytes
                      WHEN p_max_bytes = null_value
                         THEN NULL
                      ELSE p_max_bytes
                   END,
                baseurl =
                   CASE
                      WHEN p_baseurl IS NULL
                         THEN baseurl
                      WHEN p_baseurl = null_value
                         THEN NULL
                      ELSE p_baseurl
                   END,
                reject_limit =
                   CASE
                      WHEN p_reject_limit IS NULL
                         THEN reject_limit
                      WHEN p_reject_limit = null_value
                         THEN NULL
                      ELSE p_reject_limit
                   END,
                description =
                   CASE
                      WHEN p_description IS NULL
                         THEN description
                      WHEN p_description = null_value
                         THEN NULL
                      ELSE p_description
                   END,
                modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
                modified_dt = SYSDATE
          WHERE file_label = LOWER (p_file_label)
            AND file_group = LOWER (p_file_group);
		   
      -- instantiate the object to verify it
      o_extract := extract_ot ( p_file_label      => p_file_label );
      o_ev.clear_app_info;
		
   END modify_extract;
   
   PROCEDURE delete_extract (
      p_file_label         VARCHAR2
   )
   IS
      o_ev         evolve_ot   := evolve_ot (p_module      => 'delete_extract');
   BEGIN
      DELETE FROM file_conf
       WHERE file_label = LOWER (p_file_label);
      
      o_ev.clear_app_info;

   END delete_extract;

   PROCEDURE create_mapping (
      p_mapping             VARCHAR2,
      p_mapping_type        VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'no',
      p_source_owner        VARCHAR2 DEFAULT NULL,
      p_source_object       VARCHAR2 DEFAULT NULL,
      p_source_column       VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT 'transfer',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_description         VARCHAR2 DEFAULT NULL
   )
   IS
      l_num_rows   NUMBER;
      o_map        mapping_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_ev          evolve_ot     := evolve_ot (p_module  => 'create_mapping');
   BEGIN

      BEGIN
	 INSERT INTO mapping_conf
		( mapping_name, mapping_type,
		  table_owner, table_name,
		  partition_name, manage_indexes,
		  manage_constraints,
		  source_owner, source_object,
		  source_column, replace_method,
		  STATISTICS,
		  concurrent, index_regexp,
		  index_type, partition_type, constraint_regexp,
		  constraint_type, description
		)
		VALUES ( LOWER (p_mapping), LOWER (p_mapping_type),
			 UPPER (p_owner), UPPER (p_table),
			 UPPER (p_partname), LOWER (NVL (p_indexes, 'no')),
			 LOWER (p_constraints),
			 UPPER (p_source_owner), UPPER (p_source_object),
			 UPPER (p_source_column), p_replace_method,
			 LOWER (p_statistics),
			 LOWER (p_concurrent), p_index_regexp,
			 p_index_type, p_part_type, p_constraint_regexp,
			 p_constraint_type, p_description
                       );
      EXCEPTION
	 WHEN e_dup_conf
	 THEN
            o_ev.clear_app_info;
            evolve.raise_err ('dup_conf');
      END;

      -- initiating the object will run the business logic checks      
      o_map := trans_factory.get_mapping_ot (p_mapping => p_mapping);
      o_ev.clear_app_info;
   EXCEPTION
      WHEN others
      THEN
         evolve.log_err;
      	 o_ev.clear_app_info;
      	 RAISE;
   END create_mapping;

   PROCEDURE create_mapping (
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'no',
      p_source_owner        VARCHAR2 DEFAULT NULL,
      p_source_object       VARCHAR2 DEFAULT NULL,
      p_source_column       VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT 'transfer',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_description         VARCHAR2 DEFAULT NULL
   )
   IS
      o_ev          evolve_ot     := evolve_ot (p_module  => 'create_mapping');
   BEGIN
      
      create_mapping ( p_mapping   => p_mapping,
		       p_mapping_type => 'table',
		       p_owner => p_owner,
		       p_table => p_table,
		       p_partname => p_partname,
		       p_indexes => p_indexes,
		       p_constraints => p_constraints,
		       p_source_owner => p_source_owner,
		       p_source_object => p_source_object,
		       p_source_column => p_source_column,
		       p_replace_method => p_replace_method,
		       p_statistics => p_statistics,
		       p_concurrent => p_concurrent,
		       p_index_regexp => p_index_regexp,
		       p_index_type => p_index_type,
		       p_part_type => p_part_type,
		       p_constraint_regexp => p_constraint_regexp,
		       p_constraint_type => p_constraint_type,
		       p_description => p_description );
      
      o_ev.clear_app_info;

   END create_mapping;
   
   PROCEDURE modify_mapping (
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'no',
      p_source_owner        VARCHAR2 DEFAULT NULL,
      p_source_object       VARCHAR2 DEFAULT NULL,
      p_source_column       VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT 'transfer',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_description         VARCHAR2 DEFAULT NULL
   )
   IS
      l_map_type   mapping_conf.mapping_type%TYPE;
      l_num_rows   NUMBER;
      o_map        mapping_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_ev          evolve_ot     := evolve_ot (p_module      => 'modify_mapping');
   BEGIN
      
      BEGIN
         --first, check to make sure that we should be modifying this record
         SELECT mapping_type
           INTO l_map_type
           FROM mapping_conf
          WHERE LOWER (mapping_name) = LOWER (p_mapping);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record, then raise the error
	    o_ev.clear_app_info;
            evolve.raise_err( 'no_mapping' );
      END;
      
      -- if the constant null_value is used, then the value should be set to null
      UPDATE mapping_conf
         SET table_name =
             UPPER (CASE
                     WHEN p_table IS NULL
                     THEN table_name
                     WHEN p_table = null_value
                     THEN NULL
                     ELSE p_table
                     END
                   ),
             partition_name =
             UPPER (CASE
                     WHEN p_partname IS NULL
                     THEN partition_name
                     WHEN p_partname = null_value
                     THEN NULL
                     ELSE p_partname
                     END
                   ),
             manage_indexes =
             LOWER (CASE
                     WHEN p_indexes IS NULL
                     THEN manage_indexes
                     WHEN p_indexes = null_value
                     THEN NULL
                     ELSE p_indexes
                     END
                   ),
             manage_constraints =
             LOWER (CASE
                     WHEN p_constraints IS NULL
                     THEN manage_constraints
                     WHEN p_constraints = null_value
                     THEN NULL
                     ELSE p_constraints
                     END
                   ),
             source_owner =
             UPPER (CASE
                     WHEN p_source_owner IS NULL
                     THEN source_owner
                     WHEN p_source_owner = null_value
                     THEN NULL
                     ELSE p_source_owner
                     END
                   ),
             source_object =
             UPPER (CASE
                     WHEN p_source_object IS NULL
                     THEN source_object
                     WHEN p_source_object = null_value
                     THEN NULL
                     ELSE p_source_object
                     END
                   ),
             source_column =
             UPPER (CASE
                     WHEN p_source_column IS NULL
                     THEN source_column
                     WHEN p_source_column = null_value
                     THEN NULL
                     ELSE p_source_column
                     END
                   ),
             replace_method =
             LOWER (CASE
                     WHEN p_replace_method IS NULL
                     THEN replace_method
                     WHEN p_replace_method = null_value
                     THEN NULL
                     ELSE p_replace_method
                     END
                   ),
             STATISTICS =
             LOWER (CASE
                     WHEN p_statistics IS NULL
                     THEN statistics
                     WHEN p_statistics = null_value
                     THEN NULL
                     ELSE p_statistics
                     END
                   ),
             concurrent =
             LOWER (CASE
                     WHEN p_concurrent IS NULL
                     THEN concurrent
                     WHEN p_concurrent = null_value
                     THEN NULL
                     ELSE p_concurrent
                     END
                   ),
             index_regexp =
             CASE
             WHEN p_index_regexp IS NULL
             THEN index_regexp
             WHEN p_index_regexp = null_value
             THEN NULL
             ELSE p_index_regexp
             END,
             index_type =
             CASE
             WHEN p_index_type IS NULL
             THEN index_type
             WHEN p_index_type = null_value
             THEN NULL
             ELSE p_index_type
             END,
             partition_type =
             CASE
             WHEN p_part_type IS NULL
             THEN partition_type
             WHEN p_part_type = null_value
             THEN NULL
             ELSE p_part_type
             END,
             constraint_regexp =
             CASE
             WHEN p_constraint_regexp IS NULL
             THEN constraint_regexp
             WHEN p_constraint_regexp = null_value
             THEN NULL
             ELSE p_constraint_regexp
             END,
             constraint_type =
             CASE
             WHEN p_constraint_type IS NULL
             THEN constraint_type
             WHEN p_constraint_type = null_value
             THEN NULL
             ELSE p_constraint_type
             END,
             description =
             CASE
             WHEN p_description IS NULL
             THEN description
             WHEN p_description = null_value
             THEN NULL
             ELSE p_description
             END,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE mapping_name = LOWER (p_mapping);


         -- now use the dimension object to validate the new structure
         -- just constructing the object calls the CONFIRM_OBJECTS procedure
         o_map := trans_factory.get_mapping_ot (p_mapping => p_mapping);
             
         o_ev.clear_app_info;
   END modify_mapping;

   
   PROCEDURE delete_mapping (
      p_mapping             VARCHAR2
   )
   IS
      o_ev          evolve_ot     := evolve_ot (p_module      => 'delete_mapping');
   BEGIN

      -- if a delete is specifically requested, then do a delete
      DELETE FROM mapping_conf
       WHERE mapping_name = LOWER (p_mapping);
      
      o_ev.clear_app_info;

   END delete_mapping;   

   PROCEDURE delete_mapping (
      p_owner              VARCHAR2,
      p_table              VARCHAR2
   )
   IS
      l_mapping    mapping_conf.mapping_name%TYPE;
      o_ev          evolve_ot     := evolve_ot (p_module      => 'delete_mapping');
   BEGIN
      
      BEGIN

	 SELECT mapping_name
	   INTO l_mapping
	   FROM mapping_conf
	  WHERE lower( table_owner ) = lower( p_owner )
	    AND lower( table_name ) = lower( p_table );
      EXCEPTION
	 WHEN no_data_found
	 THEN
	 evolve.raise_err( 'no_dim' );
      END;
      
      delete_mapping( l_mapping );
      
      o_ev.clear_app_info;

   END delete_mapping;   

   PROCEDURE create_dimension (
      p_mapping            VARCHAR2,
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
      p_source_owner       VARCHAR2,
      p_source_object      VARCHAR2,
      p_sequence_owner     VARCHAR2,
      p_sequence_name      VARCHAR2,
      p_staging_owner      VARCHAR2 DEFAULT NULL,
      p_staging_table      VARCHAR2 DEFAULT NULL,
      p_default_scd_type   NUMBER DEFAULT 2,
      p_direct_load        VARCHAR2 DEFAULT 'yes',
      p_replace_method     VARCHAR2 DEFAULT 'rename',
      p_statistics         VARCHAR2 DEFAULT 'transfer',
      p_concurrent         VARCHAR2 DEFAULT 'no',
      p_stage_key_def      NUMBER DEFAULT -.01,
      p_char_nvl_def       VARCHAR2 DEFAULT '~',
      p_date_nvl_def       DATE DEFAULT TO_DATE ('01/01/9999'),
      p_num_nvl_def        NUMBER DEFAULT -.01,
      p_description        VARCHAR2 DEFAULT NULL
   )
   IS
      o_dim        mapping_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_conf, -1);
      o_ev         evolve_ot  := evolve_ot (p_module => 'create_dimension');
   BEGIN
      BEGIN
         INSERT INTO dimension_conf
                     (table_owner, table_name,
                      sequence_owner, sequence_name,
                      staging_owner, staging_table,
                      default_scd_type, direct_load,
                      stage_key_default, char_nvl_default,
		      date_nvl_default, number_nvl_default, description
                     )
              VALUES (UPPER (p_owner), UPPER (p_table),
                      UPPER (p_sequence_owner), UPPER (p_sequence_name),
                      UPPER (p_staging_owner), UPPER (p_staging_table),
                      p_default_scd_type, LOWER (p_direct_load),
                      p_stage_key_def, p_char_nvl_def, p_date_nvl_def,
                      p_num_nvl_def, p_description
                     );
      EXCEPTION
         WHEN e_dup_conf
         THEN
            evolve.raise_err ('dup_conf');
      END;

      -- now make the call to create the mapping
      create_mapping (p_mapping             => p_mapping,
                      p_mapping_type        => 'dimension',
                      p_table               => p_table,
                      p_owner               => p_owner,
                      p_source_owner        => p_source_owner,
                      p_source_object       => p_source_object,
                      p_replace_method      => p_replace_method,
                      p_statistics          => p_statistics,
                      p_concurrent          => p_concurrent
                     );
      o_dim := trans_factory.get_mapping_ot (p_mapping);
      
      o_ev.clear_app_info;

   END create_dimension;

   PROCEDURE modify_dimension (
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
      p_mapping            VARCHAR2 DEFAULT NULL,
      p_source_owner       VARCHAR2 DEFAULT NULL,
      p_source_object      VARCHAR2 DEFAULT NULL,
      p_sequence_owner     VARCHAR2 DEFAULT NULL,
      p_sequence_name      VARCHAR2 DEFAULT NULL,
      p_staging_owner      VARCHAR2 DEFAULT NULL,
      p_staging_table      VARCHAR2 DEFAULT NULL,
      p_default_scd_type   NUMBER DEFAULT NULL,
      p_direct_load        VARCHAR2 DEFAULT NULL,
      p_replace_method     VARCHAR2 DEFAULT NULL,
      p_statistics         VARCHAR2 DEFAULT NULL,
      p_concurrent         VARCHAR2 DEFAULT NULL,
      p_stage_key_def      NUMBER DEFAULT NULL,
      p_char_nvl_def       VARCHAR2 DEFAULT NULL,
      p_date_nvl_def       DATE DEFAULT NULL,
      p_num_nvl_def        NUMBER DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
   )
   IS
      l_mapping    mapping_conf.mapping_name%TYPE;
      o_dim   mapping_ot;
      o_ev    evolve_ot  := evolve_ot (p_module => 'modify_dimension');
   BEGIN
      UPDATE dimension_conf
         SET sequence_owner =
                UPPER (CASE
                          WHEN p_sequence_owner IS NULL
                             THEN sequence_owner
                          WHEN p_sequence_owner = null_value
                             THEN NULL
                          ELSE p_sequence_owner
                       END
                      ),
             sequence_name =
                UPPER (CASE
                          WHEN p_sequence_name IS NULL
                             THEN sequence_name
                          WHEN p_sequence_name = null_value
                             THEN NULL
                          ELSE p_sequence_name
                       END
                      ),
             staging_owner =
                UPPER (CASE
                          WHEN p_staging_owner IS NULL
                             THEN staging_owner
                          WHEN p_staging_owner = null_value
                             THEN NULL
                          ELSE p_staging_owner
                       END
                      ),
             staging_table =
                UPPER (CASE
                          WHEN p_staging_table IS NULL
                             THEN staging_table
                          WHEN p_staging_table = null_value
                             THEN NULL
                          ELSE p_staging_table
                       END
                      ),
             default_scd_type =
                CASE
                   WHEN p_default_scd_type IS NULL
                      THEN default_scd_type
                   WHEN p_default_scd_type = null_value
                      THEN NULL
                   ELSE p_default_scd_type
                END,
             direct_load =
                LOWER (CASE
                          WHEN p_direct_load IS NULL
                             THEN direct_load
                          WHEN p_direct_load = null_value
                             THEN NULL
                          ELSE p_direct_load
                       END
                      ),
             stage_key_default =
                CASE
                   WHEN p_stage_key_def IS NULL
                      THEN stage_key_default
                   WHEN p_stage_key_def = null_value
                      THEN NULL
                   ELSE p_stage_key_def
                END,
             char_nvl_default =
                CASE
                   WHEN p_char_nvl_def IS NULL
                      THEN char_nvl_default
                   WHEN p_char_nvl_def = null_value
                      THEN NULL
                   ELSE p_char_nvl_def
                END,
             date_nvl_default =
                CASE
                   WHEN p_date_nvl_def IS NULL
                      THEN date_nvl_default
                   WHEN p_date_nvl_def = null_value
                      THEN NULL
                   ELSE p_date_nvl_def
                END,
             number_nvl_default =
                CASE
                   WHEN p_num_nvl_def IS NULL
                      THEN number_nvl_default
                   WHEN p_num_nvl_def = null_value
                      THEN NULL
                   ELSE p_num_nvl_def
                END,
             description =
                CASE
                   WHEN p_description IS NULL
                      THEN description
                   WHEN p_description = null_value
                      THEN NULL
                   ELSE p_description
                END,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE LOWER (table_owner) = LOWER (p_owner)
         AND LOWER (table_name) = LOWER (p_table);

     IF sql%rowcount = 0
     THEN
	evolve.raise_err( 'no_dim' );
     END IF;

      -- get the mapping name
      o_ev.change_action ('get mapping name');

      SELECT mapping_name
        INTO l_mapping
        FROM mapping_conf
       WHERE LOWER (table_owner) = LOWER (p_owner)
         AND LOWER (table_name) = LOWER (p_table);

      -- update the mapping name in case it's been changed
      o_ev.change_action ('rename mapping name');
     
     IF p_mapping IS NOT NULL
     THEN

	UPDATE mapping_conf
           SET mapping_name = LOWER (p_mapping)
	 WHERE mapping_name = l_mapping;
	
     END IF;

      -- now make the call to modify the mapping
      modify_mapping (p_mapping             => nvl( p_mapping, l_mapping),
                      p_table               => p_table,
                      p_owner               => p_owner,
                      p_source_owner        => p_source_owner,
                      p_source_object       => p_source_object,
                      p_replace_method      => p_replace_method,
                      p_statistics          => p_statistics,
                      p_concurrent          => p_concurrent
                     );
     
     o_ev.clear_app_info;
   END modify_dimension;

   PROCEDURE delete_dimension ( p_owner VARCHAR2, p_table VARCHAR2 )
   IS
      o_ev   evolve_ot := evolve_ot (p_module => 'delete_dimension');
   BEGIN
      -- delete the column configuration
      delete_dim_attribs( p_owner => p_owner, p_table => p_table );

      -- now delete the dimension configuration
      BEGIN
	 DELETE FROM dimension_conf
	  WHERE LOWER (table_owner) = LOWER (p_owner)
            AND LOWER (table_name) = LOWER (p_table);
      EXCEPTION
	 WHEN no_data_found
	 THEN
	 evolve.raise_err( 'no_dim' );
      END;
      -- now make the call to delete the mapping
      delete_mapping ( p_owner => p_owner,
		       p_table => p_table );
      
      o_ev.clear_app_info;
   END delete_dimension;

   PROCEDURE create_dim_attribs (
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_surrogate       VARCHAR2,
      p_effective_dt    VARCHAR2,
      p_expiration_dt   VARCHAR2,
      p_current_ind     VARCHAR2,
      p_nat_key         VARCHAR2,
      p_scd1            VARCHAR2 DEFAULT NULL,
      p_scd2            VARCHAR2 DEFAULT NULL
   )
   IS
      l_mapping    mapping_conf.mapping_name%TYPE;
      l_col_list   LONG;
      o_ev   evolve_ot := evolve_ot (p_module => 'create_dim_attribs');

      -- a dimension table should have already been configured
      o_dim        mapping_ot;
   BEGIN
      -- construct a DIMENSION_OT object
      -- this is done using the supertype MAPPING_OT
      o_dim := trans_factory.get_mapping_ot ( p_owner => p_owner,
					      p_table => p_table );

      -- construct the list for instrumentation purposes      
      l_col_list :=
         UPPER (td_core.format_list (   p_surrogate
                                     || ','
                                     || p_nat_key
                                     || ','
                                     || p_scd1
                                     || ','
                                     || p_scd2
                                     || ','
                                     || p_effective_dt
                                     || ','
                                     || p_expiration_dt
                                     || ','
                                     || p_current_ind
                                    )
               );

      evolve.log_msg ('The column list: ' || l_col_list, 5);


      -- write the surrogate key information
      o_ev.change_action( 'configure surrogate key' );
      td_utils.check_column( p_owner	=> p_owner,
			     p_table	=> p_table,
			     p_column	=> p_surrogate );
      
      INSERT INTO column_conf
	     ( table_owner, table_name, column_name, column_type )
	     VALUES 
	     ( upper( p_owner ), upper( p_table ), upper( p_surrogate ), 'surrogate key' );
      
      -- record the number of rows affected by the last statment
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of surrogate keys inserted' );

      -- write the effective date information
      o_ev.change_action( 'configure effective date' );
      td_utils.check_column( p_owner	=> p_owner,
			     p_table	=> p_table,
			     p_column	=> p_effective_dt );
      
      INSERT INTO column_conf
	     ( table_owner, table_name, column_name, column_type )
	     VALUES 
	     ( upper( p_owner ), upper( p_table ), upper( p_effective_dt ), 'effective date' );
      
      -- record the number of rows affected by the last statment
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of effective dates inserted' );

      -- write the expiration date information
      o_ev.change_action( 'configure expire date' );
      td_utils.check_column( p_owner	=> p_owner,
			     p_table	=> p_table,
			     p_column	=> p_expiration_dt );
      
      INSERT INTO column_conf
	     ( table_owner, table_name, column_name, column_type )
	     VALUES 
	     ( upper( p_owner ), upper( p_table ), upper( p_expiration_dt ), 'expiration date' );

      -- record the number of rows affected by the last statment
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of expiration dates inserted' );
      
      -- write the current indicator information
      o_ev.change_action( 'configure current indicator' );
      td_utils.check_column( p_owner	=> p_owner,
			     p_table	=> p_table,
			     p_column	=> p_current_ind );
      
      INSERT INTO column_conf
	     ( table_owner, table_name, column_name, column_type )
	     VALUES 
	     ( upper( p_owner ), upper( p_table ), upper( p_current_ind ), 'current indicator' );

      -- record the number of rows affected by the last statment
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of current indicators inserted' );
      
      -- write the natural key information
      o_ev.change_action( 'configure natural key' );
      FOR c_cols IN (SELECT COLUMN_VALUE column_name
                       FROM TABLE (CAST (td_core.SPLIT (p_nat_key, ',') AS split_ot
                                        )
                                  ))
      LOOP
	 
	 evolve.log_msg( 'The natural key column being configured is: '||c_cols.column_name, 5 );
         td_utils.check_column (p_owner       => p_owner,
                                p_table       => p_table,
                                p_column      => c_cols.column_name
                               );

	 INSERT INTO column_conf
		( table_owner, table_name, column_name, column_type )
		VALUES 
		( upper( p_owner ), upper( p_table ), upper( c_cols.column_name ), 'natural key' );
	 
	 -- record the number of rows affected by the last statment
	 evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of natural keys inserted' );

      END LOOP;
      
      -- write the type 1 attributes
      o_ev.change_action( 'configure scd1' );
      
      -- only run the loop process if the p_scd1 column is not null
      IF p_scd1 IS NOT NULL
      THEN

	 FOR c_cols IN (SELECT COLUMN_VALUE column_name
			  FROM TABLE (CAST (td_core.SPLIT (p_scd1, ',') AS split_ot
                                           )
                                     ))
	 LOOP
	    evolve.log_msg( 'The scd1 column being configured is: '||c_cols.column_name, 5 );
            td_utils.check_column (p_owner       => p_owner,
                                    p_table       => p_table,
                                    p_column      => c_cols.column_name
				  );

	    INSERT INTO column_conf
		   ( table_owner, table_name, column_name, column_type )
		   VALUES 
		   ( upper( p_owner ), upper( p_table ), upper( c_cols.column_name ), 'scd type 1' );
	    
	    -- record the number of rows affected by the last statment
	    evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of scd1 attributes inserted' );

	 END LOOP;
	 
      END IF;
      
      -- write the type 2 attributes
      
      -- only do the loop process if p_scd2 is not null
      IF p_scd2 IS NOT NULL
      THEN

	 o_ev.change_action( 'configure scd2' );
	 FOR c_cols IN (SELECT COLUMN_VALUE column_name
			  FROM TABLE (CAST (td_core.SPLIT (p_scd2, ',') AS split_ot
                                           )
                                     ))
	 LOOP
	    evolve.log_msg( 'The scd2 column being configured is: '||c_cols.column_name, 5 );
            td_utils.check_column (p_owner       => p_owner,
                                    p_table       => p_table,
                                    p_column      => c_cols.column_name
				  );

	    INSERT INTO column_conf
		   ( table_owner, table_name, column_name, column_type )
		   VALUES 
		   ( upper( p_owner ), upper( p_table ), upper( c_cols.column_name ), 'scd type 2' );

	    -- record the number of rows affected by the last statment
	    evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of scd2 attributes inserted' );
	    
	 END LOOP;
      END IF;
      
      -- EXECUTE the merge statement to write any columns that have been left off
      MERGE INTO column_conf t
         USING ( SELECT table_owner, dc.table_name, column_name,
                        CASE default_scd_type
                           WHEN 1
                              THEN 'scd type 1'
                           ELSE 'scd type 2'
                        END column_type
                  FROM all_tab_columns atc JOIN dimension_conf dc
                       ON atc.owner = dc.table_owner AND atc.table_name = dc.table_name
                 WHERE table_owner = UPPER( p_owner ) AND dc.table_name = UPPER( p_table )) s
         ON (t.table_owner = s.table_owner AND t.table_name = s.table_name AND t.column_name = s.column_name )
         WHEN NOT MATCHED THEN
            INSERT( t.table_owner, t.table_name, t.column_name, t.column_type )
            VALUES( s.table_owner, s.table_name, s.column_name, s.column_type );
      
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4, p_msg => 'Number of rows merged' );


      -- confirm the dimension columns
      o_dim.confirm_dim_cols;
            
      o_ev.clear_app_info;
   END create_dim_attribs;
   
   PROCEDURE modify_dim_attrib (
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_column		VARCHAR2,
      p_column_type	VARCHAR2
   )
   IS
      l_mapping    mapping_conf.mapping_name%TYPE;
      o_ev   evolve_ot := evolve_ot (p_module => 'modify_dim_attrib');

      -- a dimension table should have already been configured
      o_dim        mapping_ot;
   BEGIN
      -- construct a DIMENSION_OT object
      -- this is done using the supertype MAPPING_OT
      o_dim := trans_factory.get_mapping_ot ( p_owner => p_owner,
					      p_table => p_table );

      evolve.log_msg ('The column being modified: ' || p_column, 5);

      -- modify the attribute type
      o_ev.change_action( 'modify attribute type' );
      td_utils.check_column( p_owner	=> p_owner,
			     p_table	=> p_table,
			     p_column	=> p_column );
      
      UPDATE column_conf
	 SET column_type = p_column_type
       WHERE lower( table_owner ) = lower( p_owner )
	 AND lower( table_name ) = lower( p_table )
	 AND lower( column_name ) = lower( p_column );
      
      -- record the number of rows affected by the last statment
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4 );
      
      -- confirm the dimension columns
      o_dim.confirm_dim_cols;
      o_ev.clear_app_info;
   END modify_dim_attrib;
   
   PROCEDURE delete_dim_attribs (
      p_owner           VARCHAR2,
      p_table           VARCHAR2
   )
   IS
   BEGIN
      
      -- delete the column configuration
      DELETE FROM column_conf
            WHERE LOWER (table_owner) = LOWER (p_owner)
         AND LOWER (table_name) = LOWER (p_table);
      
      -- record the number of rows affected by the last statment
      evolve.log_cnt_msg( SQL%ROWCOUNT, p_level => 4 );
      
      o_ev.clear_app_info;
   END delete_dim_attribs;

END trans_adm;
/

SHOW errors