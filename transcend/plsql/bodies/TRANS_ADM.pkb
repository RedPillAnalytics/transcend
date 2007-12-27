CREATE OR REPLACE PACKAGE BODY trans_adm
IS
   PROCEDURE set_default_configs
   IS
   BEGIN
      -- set the notification events
      evolve_adm.set_notification_event( 'audit_file',
                                         'file too large',
                                         'File outside size threshholds',
                                         'The file referenced below is larger than the configured threshhold:'
                                       );
      evolve_adm.set_notification_event( 'audit_file',
                                         'file too small',
                                         'File outside size threshholds',
                                         'The file referenced below is smaller than the configured threshhold:'
                                       );
      -- load the entries into the ERROR_CONF table for Transcend
      evolve_adm.set_error_conf( p_name => 'no_files_found', p_message => 'No files found for this configuration' );
      evolve_adm.set_error_conf( p_name         => 'no_ext_files',
                                 p_message      => 'There are no files found for this external table'
                               );
      evolve_adm.set_error_conf( p_name         => 'reject_limit_exceeded',
                                 p_message      => 'The external table reject limit was exceeded'
                               );
      evolve_adm.set_error_conf( p_name         => 'ext_file_missing',
                                 p_message      => 'The physical file for the specified external table does not exist'
                               );
      evolve_adm.set_error_conf( p_name         => 'fail_source_policy',
                                 p_message      => 'Multiple matching files found with a SOURCE_POLICY value of "fail"'
                               );
      evolve_adm.set_error_conf
         ( p_name         => 'on_clause_missing',
           p_message      => 'Either a unique constraint must exist on the target table, or a value for P_COLUMNS must be specified'
         );
      evolve_adm.set_error_conf( p_name         => 'notify_err',
                                 p_message      => 'There is an error with configuration for the specified notification'
                               );
      evolve_adm.set_error_conf( p_name         => 'incorrect_parameters',
                                 p_message      => 'The combination of parameters provided yields no matching objects'
                               );
      evolve_adm.set_error_conf( p_name         => 'file_too_big',
                                 p_message      => 'The specified file is larger than the MAX_BYTES parameter'
                               );
      evolve_adm.set_error_conf( p_name         => 'file_too_small',
                                 p_message      => 'The specified file is smaller than the MAX_BYTES parameter'
                               );
      evolve_adm.set_error_conf( p_name => 'no_stats', p_message => 'The specified segment has no stored statistics' );
      evolve_adm.set_error_conf
         ( p_name         => 'data_cartridge',
           p_message      => 'An unregistered data cartridge error was returned while selecting from the specified external table'
         );
      evolve_adm.set_error_conf( p_name         => 'multi_loc_ext_tab',
                                 p_message      => 'External tables used in Transcend Files must contain a single location'
                               );
      evolve_adm.set_error_conf( p_name => 'no_ext_tab', p_message => 'The specified external table does not exist' );
      evolve_adm.set_error_conf( p_name         => 'parms_combo',
                                 p_message      => 'The specified parameters are mutually inclusive' );
      evolve_adm.set_error_conf( p_name         => 'no_dim',
                                 p_message      => 'The specified table is not a configured dimension table'
                               );
   END set_default_configs;

   PROCEDURE configure_feed(
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_table_owner        VARCHAR2 DEFAULT NULL,
      p_table_name         VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT NULL,
      p_max_bytes          NUMBER DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_regexp_options     VARCHAR2 DEFAULT NULL,
      p_source_policy      VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT NULL,
      p_reject_limit       NUMBER DEFAULT NULL,
      p_file_description   VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   )
   IS
      l_owner       all_external_tables.owner%TYPE;
      l_table       all_external_tables.table_name%TYPE;
      l_dir_path    all_directories.directory_path%TYPE;
      l_directory   all_external_tables.default_directory_name%TYPE;
      l_ext_tab     all_external_tables.table_name%TYPE;
      e_dup_conf    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      CASE
         WHEN    ( p_table_owner IS NULL AND p_table_name IS NOT NULL )
              OR ( p_table_owner IS NOT NULL AND p_table_name IS NULL )
         THEN
            evolve_log.raise_err( 'parms_comb', 'P_TABLE_OWNER and P_TABLE_NAME' );
         WHEN p_table_owner IS NOT NULL
         THEN
            -- table information is provided, so use that
            l_owner := UPPER( p_table_owner );
            l_table := UPPER( p_table_name );
            -- now check the external table
            td_utils.check_table( p_owner => p_table_owner, p_table => p_table_name, p_external => 'yes' );
         WHEN p_table_owner IS NULL
         THEN
            -- the object_owner is null, so we need to pull the table information from the configuration
            SELECT UPPER( object_owner ), UPPER( object_name )
              INTO l_owner, l_table
              FROM files_conf
             WHERE file_group = LOWER( p_file_group ) AND file_label = LOWER( p_file_label );
      END CASE;

      -- do checks to make sure all the provided information is legitimate
      IF NOT p_mode = 'delete'
      THEN
         -- check to see if the directories are legitimate
         -- if they aren't, the GET_DIR_PATH function raises an error
         IF p_arch_directory IS NOT NULL
         THEN
            l_dir_path := td_utils.get_dir_path( p_arch_directory );
         END IF;

         IF p_source_directory IS NOT NULL
         THEN
            l_dir_path := td_utils.get_dir_path( p_source_directory );
         END IF;

         -- get the directory from the external table
         BEGIN
            SELECT default_directory_name
              INTO l_directory
              FROM all_external_tables
             WHERE owner = l_owner AND table_name = l_table;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               evolve_log.raise_err( 'no_ext_tab', UPPER( l_owner || '.' || l_table ));
         END;
      END IF;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE files_conf
            SET file_description = NVL( p_file_description, file_description ),
                object_owner = UPPER( NVL( p_table_owner, object_owner )),
                object_name = UPPER( NVL( p_table_name, object_name )),
                DIRECTORY = l_directory,
                filename = NVL( p_filename, filename ),
                arch_directory = UPPER( NVL( p_arch_directory, arch_directory )),
                min_bytes = NVL( p_min_bytes, min_bytes ),
                max_bytes = NVL( p_max_bytes, max_bytes ),
                file_datestamp = NVL( p_file_datestamp, file_datestamp ),
                baseurl = NVL( p_baseurl, baseurl ),
                passphrase = NVL( p_passphrase, passphrase ),
                source_directory = UPPER( NVL( p_source_directory, source_directory )),
                source_regexp = NVL( p_source_regexp, source_regexp ),
                regexp_options = NVL( p_regexp_options, regexp_options ),
                source_policy = NVL( p_source_policy, source_policy ),
                required = NVL( p_required, required ),
                delete_source = NVL( p_delete_source, delete_source ),
                reject_limit = NVL( p_reject_limit, reject_limit ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE file_label = LOWER( p_file_label ) AND file_group = LOWER( p_file_group );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         CASE
            WHEN p_table_owner IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_TABLE_OWNER' );
            WHEN p_table_name IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_TABLE_NAME' );
            WHEN p_arch_directory IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_ARCH_DIRECTORY' );
            WHEN p_source_directory IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SOURCE_DIRECTORY' );
            WHEN p_source_regexp IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SOURCE_REGEXP' );
            ELSE
               NULL;
         END CASE;

         BEGIN
            INSERT INTO files_conf
                        ( file_label, file_group, file_type, file_description, object_owner,
                          object_name, DIRECTORY, filename, arch_directory,
                          min_bytes, max_bytes, file_datestamp, baseurl, passphrase,
                          source_directory, source_regexp, regexp_options,
                          source_policy, required, delete_source,
                          reject_limit
                        )
                 VALUES ( p_file_label, p_file_group, 'feed', p_file_description, UPPER( p_table_owner ),
                          UPPER( p_table_name ), l_directory, p_filename, UPPER( p_arch_directory ),
                          NVL( p_min_bytes, 0 ), NVL( p_max_bytes, 0 ), p_file_datestamp, p_baseurl, p_passphrase,
                          UPPER( p_source_directory ), p_source_regexp, NVL( p_regexp_options, 'i' ),
                          NVL( p_source_policy, 'newest' ), NVL( p_required, 'yes' ), NVL( p_delete_source, 'yes' ),
                          NVL( p_reject_limit, 100 )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve_log.raise_err( 'dup_conf' );
         END;
      END IF;

      IF LOWER( p_mode ) = 'delete'
      THEN
         -- if a delete is specifically requested, then do a delete
         DELETE FROM files_conf
               WHERE file_label = LOWER( p_file_label ) AND file_group = LOWER( p_file_group );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         evolve_log.raise_err( 'no_rep_obj' );
      END IF;
   END configure_feed;

   PROCEDURE configure_extract(
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT NULL,
      p_max_bytes          NUMBER DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT NULL,
      p_timestampformat    VARCHAR2 DEFAULT NULL,
      p_delimiter          VARCHAR2 DEFAULT NULL,
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT NULL,
      p_file_description   VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   )
   IS
      l_owner      all_external_tables.owner%TYPE;
      l_object     all_objects.object_name%TYPE;
      l_dir_path   all_directories.directory_path%TYPE;
      l_obj_name   VARCHAR2( 61 )                        := p_object_owner || '.' || p_object_name;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- do checks to make sure all the provided information is legitimate
      IF NOT p_mode = 'delete'
      THEN
         -- check to see if the directories are legitimate
         -- if they aren't, the GET_DIR_PATH function raises an error
         IF p_arch_directory IS NOT NULL
         THEN
            l_dir_path := td_utils.get_dir_path( p_arch_directory );
         END IF;

         IF p_directory IS NOT NULL
         THEN
            l_dir_path := td_utils.get_dir_path( p_directory );
         END IF;
      END IF;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE files_conf
            SET file_description = NVL( p_file_description, file_description ),
                object_owner = UPPER( NVL( p_object_owner, object_owner )),
                object_name = UPPER( NVL( p_object_name, object_name )),
                DIRECTORY = UPPER( NVL( p_directory, DIRECTORY )),
                filename = NVL( p_filename, filename ),
                arch_directory = UPPER( NVL( p_arch_directory, arch_directory )),
                min_bytes = NVL( p_min_bytes, min_bytes ),
                max_bytes = NVL( p_max_bytes, max_bytes ),
                file_datestamp = NVL( p_file_datestamp, file_datestamp ),
                baseurl = NVL( p_baseurl, baseurl ),
                passphrase = NVL( p_passphrase, passphrase ),
                DATEFORMAT = NVL( p_dateformat, DATEFORMAT ),
                timestampformat = NVL( p_timestampformat, timestampformat ),
                delimiter = NVL( p_delimiter, delimiter ),
                quotechar = NVL( p_quotechar, quotechar ),
                headers = NVL( p_headers, headers ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE file_label = LOWER( p_file_label ) AND file_group = LOWER( p_file_group );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         CASE
            WHEN p_filename IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_FILENAME' );
            WHEN p_object_owner IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OBJECT_OWNER' );
            WHEN p_object_name IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OBJECT_NAME' );
            WHEN p_directory IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_DIRECTORY' );
            WHEN p_arch_directory IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_ARCH_DIRECTORY' );
            ELSE
               NULL;
         END CASE;

         BEGIN
            INSERT INTO files_conf
                        ( file_label, file_group, file_type, file_description, object_owner,
                          object_name, DIRECTORY, filename, arch_directory,
                          min_bytes, max_bytes, file_datestamp, baseurl, passphrase,
                          DATEFORMAT,
                          timestampformat, delimiter,
                          quotechar, headers
                        )
                 VALUES ( p_file_label, p_file_group, 'extract', p_file_description, UPPER( p_object_owner ),
                          UPPER( p_object_name ), UPPER( p_directory ), p_filename, UPPER( p_arch_directory ),
                          NVL( p_min_bytes, 0 ), NVL( p_max_bytes, 0 ), p_file_datestamp, p_baseurl, p_passphrase,
                          NVL( p_dateformat, 'mm/dd/yyyy hh:mi:ss am' ),
                          NVL( p_timestampformat, 'mm/dd/yyyy hh:mi:ss:x:ff am' ), NVL( p_delimiter, ',' ),
                          p_quotechar, NVL( p_headers, 'yes' )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve_log.raise_err( 'dup_conf' );
         END;
      END IF;

      IF LOWER( p_mode ) = 'delete'
      THEN
         -- if a delete is specifically requested, then do a delete
         DELETE FROM files_conf
               WHERE file_label = LOWER( p_file_label ) AND file_group = LOWER( p_file_group );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         evolve_log.raise_err( 'no_rep_obj' );
      END IF;
   END configure_extract;

   PROCEDURE configure_dim(
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
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
      p_description        VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   )
   IS
      o_dim        dimension_ot;
      l_num_rows   NUMBER;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         evolve_log.log_msg( 'Updating configuration', 5 );

         -- first try to update an existing configuration
         UPDATE dimension_conf
            SET owner = UPPER( NVL( p_owner, owner )),
                table_name = UPPER( NVL( p_table, table_name )),
                source_owner = UPPER( NVL( p_source_owner, source_owner )),
                source_object = UPPER( NVL( p_source_object, source_object )),
                sequence_owner = UPPER( NVL( p_sequence_owner, sequence_owner )),
                sequence_name = UPPER( NVL( p_sequence_name, sequence_name )),
                staging_owner = UPPER( NVL( p_staging_owner, staging_owner )),
                staging_table = UPPER( NVL( p_staging_table, staging_table )),
                default_scd_type = NVL( p_default_scd_type, default_scd_type ),
                direct_load = LOWER( NVL( p_direct_load, direct_load )),
                replace_method = LOWER( NVL( p_replace_method, replace_method )),
                STATISTICS = LOWER( NVL( p_statistics, STATISTICS )),
                concurrent = LOWER( NVL( p_concurrent, concurrent )),
                description = NVL( p_description, description ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

         -- get the SQL rowcount
         l_num_rows := SQL%ROWCOUNT;
      END IF;

      -- updating a current config has failed, or an insert was specified
      -- in this case, insert a new record
      IF ( l_num_rows = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         evolve_log.log_msg( 'Inserting configuration', 5 );

         CASE
            WHEN p_owner IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OWNER' );
            WHEN p_table IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_TABLE_NAME' );
            WHEN p_source_owner IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SOURCE_OWNER' );
            WHEN p_source_object IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SOURCE_OBJECT' );
            WHEN p_sequence_owner IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SEQUENCE_OWNER' );
            WHEN p_sequence_name IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SEQUENCE_NAME' );
            ELSE
               NULL;
         END CASE;

         BEGIN
            INSERT INTO dimension_conf
                        ( owner, table_name, source_owner, source_object,
                          sequence_owner, sequence_name, staging_owner,
                          staging_table, default_scd_type, direct_load,
                          replace_method, STATISTICS,
                          concurrent, description
                        )
                 VALUES ( UPPER( p_owner ), UPPER( p_table ), UPPER( p_source_owner ), UPPER( p_source_object ),
                          UPPER( p_sequence_owner ), UPPER( p_sequence_name ), UPPER( p_staging_owner ),
                          UPPER( p_staging_table ), NVL( p_default_scd_type, 2 ), LOWER( NVL( p_direct_load, 'yes' )),
                          LOWER( NVL( p_replace_method, 'rename' )), LOWER( NVL( p_statistics, 'transfer' )),
                          LOWER( NVL( p_concurrent, 'yes' )), p_description
                        );

            -- get the SQL rowcount
            l_num_rows := SQL%ROWCOUNT;
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve_log.raise_err( 'dup_conf' );
         END;
      END IF;

      IF LOWER( p_mode ) = 'delete'
      THEN
         -- if a delete is specifically requested, then do a delete
         DELETE FROM dimension_conf
               WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

         -- get the SQL rowcount
         l_num_rows := SQL%ROWCOUNT;
      ELSE
              -- as long as P_MODE wasn't 'delete', then we should validate the new structure of the dimension
              -- now use the dimension object to validate the new structure
         -- just constructing the object calls the CONFIRM_OBJECTS procedure
         BEGIN
            NULL;
            o_dim := dimension_ot( p_owner => p_owner, p_table => p_table );
         END;
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF l_num_rows = 0
      THEN
         evolve_log.raise_err( 'no_rep_obj' );
      END IF;
   END configure_dim;

   PROCEDURE configure_dim_col(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_surrogate       VARCHAR2,
      p_nat_key         VARCHAR2,
      p_scd1            VARCHAR2 DEFAULT NULL,
      p_scd2            VARCHAR2 DEFAULT NULL,
      p_effective_dt    VARCHAR2 DEFAULT 'effective_dt',
      p_expiration_dt   VARCHAR2 DEFAULT 'expiration_dt',
      p_current_ind     VARCHAR2 DEFAULT 'current_ind',
      p_description     VARCHAR2 DEFAULT NULL,
      p_mode            VARCHAR2 DEFAULT 'upsert'
   )
   IS
      o_dim        dimension_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      NULL;
   END configure_dim_col;
END trans_adm;
/

SHOW errors