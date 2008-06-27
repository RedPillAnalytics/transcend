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
      evolve_adm.set_error_conf
         ( p_name         => 'dim_map_conf',
           p_message      => 'The mapping you are trying to configure is a dimensional load mapping. Use the procedure CONFIGURE_DIM to modify this configuration.'
         );
      evolve_adm.set_error_conf
         ( p_name         => 'dim_mismatch',
           p_message      => 'There is a mismatch between columns in the source object and dimension table for the specified dimension table'
         );
   END set_default_configs;

   PROCEDURE configure_feed(
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT NULL,
      p_max_bytes          NUMBER DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_match_parameter    VARCHAR2 DEFAULT NULL,
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
      l_no_conf     BOOLEAN;
      e_dup_conf    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
      o_feed        feed_ot;
      o_ev          evolve_ot                                         := evolve_ot( p_module => 'configure_feed' );
   BEGIN
      CASE
         WHEN ( p_owner IS NULL AND p_table IS NOT NULL ) OR( p_owner IS NOT NULL AND p_table IS NULL )
         THEN
            evolve_log.raise_err( 'parms_comb', 'P_OWNER and P_TABLE' );
         WHEN p_table IS NOT NULL
         THEN
            -- directory information is not configured by the user... instead, it is pulled from the external table
            -- but I don't want to make the configuring user have to provide the owner and name of the table for every configuration
            -- if it's provided, we will use that to get the directory
            -- if it isn't provided, then I will pull that information, and use it to get the directory.
            l_owner    := UPPER( p_owner );
            l_table    := UPPER( p_table );
            -- now check the external table
            td_utils.check_table( p_owner => p_owner, p_table => p_table, p_external => 'yes' );
         WHEN p_table IS NULL
         THEN
            evolve_log.log_msg( 'P_TABLE is NULL', 5 );

            -- the object_name is null, so we need to pull the table information from the configuration
            BEGIN
               SELECT UPPER( object_owner ), UPPER( object_name )
                 INTO l_owner, l_table
                 FROM files_conf
                WHERE file_group = LOWER( p_file_group ) AND file_label = LOWER( p_file_label );
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  l_no_conf    := TRUE;
            END;

            evolve_log.log_msg( 'Values for L_OWNER and L_TABLE: ' || l_owner || ',' || l_table, 5 );
      END CASE;

      -- as long as this is not a delete, I need the directory name
      IF NOT p_mode = 'delete'
      THEN
         -- get the directory from the external table
         BEGIN
            SELECT default_directory_name
              INTO l_directory
              FROM all_external_tables
             WHERE owner = l_owner AND table_name = l_table;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               IF l_no_conf
               THEN
                  NULL;
               ELSE
                  evolve_log.raise_err( 'no_ext_tab', UPPER( l_owner || '.' || l_table ));
               END IF;
         END;
      END IF;

      -- this step is used to nullify records when needed
      IF LOWER( p_mode ) = 'nullify'
      THEN
         -- this is a list of the required parameters
         CASE
            WHEN p_owner = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OWNER' );
            WHEN p_table = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_TABLE' );
            WHEN p_filename IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_FILENAME' );
            WHEN p_arch_directory = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_ARCH_DIRECTORY' );
            WHEN p_source_directory = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SOURCE_DIRECTORY' );
            WHEN p_source_regexp = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_SOURCE_REGEXP' );
            ELSE
               NULL;
         END CASE;

         UPDATE files_conf
            SET file_description =
                   CASE
                      WHEN p_file_description IS NULL
                         THEN file_description
                      WHEN p_file_description = 'NULL'
                         THEN NULL
                      ELSE file_description
                   END,
                filename = CASE
                             WHEN p_filename IS NULL
                                THEN filename
                             WHEN p_filename = 'NULL'
                                THEN NULL
                             ELSE filename
                          END,
                min_bytes = CASE
                              WHEN p_min_bytes IS NULL
                                 THEN min_bytes
                              WHEN p_min_bytes = 'NULL'
                                 THEN NULL
                              ELSE min_bytes
                           END,
                max_bytes = CASE
                              WHEN p_max_bytes IS NULL
                                 THEN max_bytes
                              WHEN p_max_bytes = 'NULL'
                                 THEN NULL
                              ELSE max_bytes
                           END,
                file_datestamp =
                   CASE
                      WHEN p_file_datestamp IS NULL
                         THEN file_datestamp
                      WHEN p_file_datestamp = 'NULL'
                         THEN NULL
                      ELSE file_datestamp
                   END,
                baseurl = CASE
                            WHEN p_baseurl IS NULL
                               THEN baseurl
                            WHEN p_baseurl = 'NULL'
                               THEN NULL
                            ELSE baseurl
                         END,
                passphrase =
                          CASE
                             WHEN p_passphrase IS NULL
                                THEN passphrase
                             WHEN p_passphrase = 'NULL'
                                THEN NULL
                             ELSE passphrase
                          END,
                match_parameter =
                   CASE
                      WHEN p_match_parameter IS NULL
                         THEN match_parameter
                      WHEN p_match_parameter = 'NULL'
                         THEN NULL
                      ELSE match_parameter
                   END,
                source_policy =
                   CASE
                      WHEN p_source_policy IS NULL
                         THEN source_policy
                      WHEN p_source_policy = 'NULL'
                         THEN NULL
                      ELSE source_policy
                   END,
                required = CASE
                             WHEN p_required IS NULL
                                THEN required
                             WHEN p_required = 'NULL'
                                THEN NULL
                             ELSE required
                          END,
                delete_source =
                   CASE
                      WHEN p_delete_source IS NULL
                         THEN delete_source
                      WHEN p_delete_source = 'NULL'
                         THEN NULL
                      ELSE delete_source
                   END,
                reject_limit =
                   CASE
                      WHEN p_reject_limit IS NULL
                         THEN reject_limit
                      WHEN p_reject_limit = 'NULL'
                         THEN NULL
                      ELSE reject_limit
                   END,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE file_label = LOWER( p_file_label ) AND file_group = LOWER( p_file_group );
      END IF;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE files_conf
            SET file_description = NVL( p_file_description, file_description ),
                object_owner = UPPER( NVL( p_owner, object_owner )),
                object_name = UPPER( NVL( p_table, object_name )),
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
                match_parameter = NVL( p_match_parameter, match_parameter ),
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
         -- this is a list of the required parameters
         CASE
            WHEN p_owner IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OWNER' );
            WHEN p_table IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_TABLE' );
            WHEN p_filename IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_FILENAME' );
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
                        ( file_label, file_group, file_type, file_description, object_owner, object_name,
                          DIRECTORY, filename, arch_directory, min_bytes,
                          max_bytes, file_datestamp, baseurl, passphrase,
                          source_directory, source_regexp, match_parameter,
                          source_policy, required, delete_source,
                          reject_limit
                        )
                 VALUES ( p_file_label, p_file_group, 'feed', p_file_description, UPPER( p_owner ), UPPER( p_table ),
                          l_directory, p_filename, UPPER( p_arch_directory ), NVL( p_min_bytes, 0 ),
                          NVL( p_max_bytes, 0 ), p_file_datestamp, p_baseurl, p_passphrase,
                          UPPER( p_source_directory ), p_source_regexp, NVL( p_match_parameter, 'i' ),
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
      ELSE
         o_feed    := feed_ot( p_file_group => p_file_group, p_file_label => p_file_label );
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
      o_extract    extract_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
      o_ev         evolve_ot                             := evolve_ot( p_module => 'configure_extract' );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) = 'nullify'
      THEN
         -- this is a list of required parameters
         CASE
            WHEN p_filename = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_FILENAME' );
            WHEN p_object_owner = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OBJECT_OWNER' );
            WHEN p_object_name = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_OBJECT_NAME' );
            WHEN p_directory = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_DIRECTORY' );
            WHEN p_arch_directory = 'NULL'
            THEN
               evolve_log.raise_err( 'parm_req', 'P_ARCH_DIRECTORY' );
            ELSE
               NULL;
         END CASE;

         UPDATE files_conf
            SET file_description =
                   CASE
                      WHEN p_file_description IS NULL
                         THEN file_description
                      WHEN p_file_description = 'NULL'
                         THEN NULL
                      ELSE file_description
                   END,
                min_bytes = CASE
                              WHEN p_min_bytes IS NULL
                                 THEN min_bytes
                              WHEN p_min_bytes = 'NULL'
                                 THEN NULL
                              ELSE min_bytes
                           END,
                max_bytes = CASE
                              WHEN p_max_bytes IS NULL
                                 THEN max_bytes
                              WHEN p_max_bytes = 'NULL'
                                 THEN NULL
                              ELSE max_bytes
                           END,
                file_datestamp =
                   CASE
                      WHEN p_file_datestamp IS NULL
                         THEN file_datestamp
                      WHEN p_file_datestamp = 'NULL'
                         THEN NULL
                      ELSE file_datestamp
                   END,
                baseurl = CASE
                            WHEN p_baseurl IS NULL
                               THEN baseurl
                            WHEN p_baseurl = 'NULL'
                               THEN NULL
                            ELSE baseurl
                         END,
                passphrase =
                          CASE
                             WHEN p_passphrase IS NULL
                                THEN passphrase
                             WHEN p_passphrase = 'NULL'
                                THEN NULL
                             ELSE passphrase
                          END,
                DATEFORMAT =
                          CASE
                             WHEN p_dateformat IS NULL
                                THEN DATEFORMAT
                             WHEN p_dateformat = 'NULL'
                                THEN NULL
                             ELSE DATEFORMAT
                          END,
                timestampformat =
                   CASE
                      WHEN p_timestampformat IS NULL
                         THEN timestampformat
                      WHEN p_timestampformat = 'NULL'
                         THEN NULL
                      ELSE timestampformat
                   END,
                delimiter = CASE
                              WHEN p_delimiter IS NULL
                                 THEN delimiter
                              WHEN p_delimiter = 'NULL'
                                 THEN NULL
                              ELSE delimiter
                           END,
                quotechar = CASE
                              WHEN p_quotechar IS NULL
                                 THEN quotechar
                              WHEN p_quotechar = 'NULL'
                                 THEN NULL
                              ELSE quotechar
                           END,
                headers = CASE
                            WHEN p_headers IS NULL
                               THEN headers
                            WHEN p_headers = 'NULL'
                               THEN NULL
                            ELSE headers
                         END,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE file_label = LOWER( p_file_label ) AND file_group = LOWER( p_file_group );
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
         -- this is a list of required parameters
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
      ELSE
         o_extract    := extract_ot( p_file_group => p_file_group, p_file_label => p_file_label );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         evolve_log.raise_err( 'no_rep_obj' );
      END IF;
   END configure_extract;

   PROCEDURE configure_mapping(
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
      p_description         VARCHAR2 DEFAULT NULL,
      p_mode                VARCHAR2 DEFAULT 'upsert'
   )
   IS
      l_num_rows   NUMBER;
      o_map        mapping_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- remove null values if P_MODE is 'nullify'
      -- any attributes specified with a 'NULL' will be nullified
      IF LOWER( p_mode ) = 'nullify'
      THEN
         UPDATE mapping_conf
            SET mapping_type = LOWER( p_mapping_type ),
                table_owner =
                         UPPER( CASE
                                   WHEN p_owner IS NULL
                                      THEN table_owner
                                   WHEN p_owner = 'NULL'
                                      THEN NULL
                                   ELSE table_owner
                                END ),
                table_name =
                           UPPER( CASE
                                     WHEN p_table IS NULL
                                        THEN table_name
                                     WHEN p_table = 'NULL'
                                        THEN NULL
                                     ELSE table_name
                                  END ),
                partition_name =
                   UPPER( CASE
                             WHEN p_partname IS NULL
                                THEN partition_name
                             WHEN p_partname = 'NULL'
                                THEN NULL
                             ELSE partition_name
                          END
                        ),
                manage_indexes =
                   LOWER( CASE
                             WHEN p_indexes IS NULL
                                THEN manage_indexes
                             WHEN p_indexes = 'NULL'
                                THEN NULL
                             ELSE manage_indexes
                          END
                        ),
                manage_constraints =
                   LOWER( CASE
                             WHEN p_constraints IS NULL
                                THEN manage_constraints
                             WHEN p_constraints = 'NULL'
                                THEN NULL
                             ELSE manage_constraints
                          END
                        ),
                source_owner =
                   UPPER( CASE
                             WHEN p_source_owner IS NULL
                                THEN source_owner
                             WHEN p_source_owner = 'NULL'
                                THEN NULL
                             ELSE source_owner
                          END
                        ),
                source_object =
                   UPPER( CASE
                             WHEN p_source_object IS NULL
                                THEN source_object
                             WHEN p_source_object = 'NULL'
                                THEN NULL
                             ELSE source_object
                          END
                        ),
                source_column =
                   UPPER( CASE
                             WHEN p_source_column IS NULL
                                THEN source_column
                             WHEN p_source_column = 'NULL'
                                THEN NULL
                             ELSE source_column
                          END
                        ),
                replace_method =
                   LOWER( CASE
                             WHEN p_replace_method IS NULL
                                THEN replace_method
                             WHEN p_replace_method = 'NULL'
                                THEN NULL
                             ELSE replace_method
                          END
                        ),
                STATISTICS =
                   LOWER( CASE
                             WHEN p_statistics IS NULL
                                THEN STATISTICS
                             WHEN p_statistics = 'NULL'
                                THEN NULL
                             ELSE STATISTICS
                          END ),
                concurrent =
                   LOWER( CASE
                             WHEN p_concurrent IS NULL
                                THEN concurrent
                             WHEN p_concurrent = 'NULL'
                                THEN NULL
                             ELSE concurrent
                          END ),
                index_regexp =
                   CASE
                      WHEN p_index_regexp IS NULL
                         THEN index_regexp
                      WHEN p_index_regexp = 'NULL'
                         THEN NULL
                      ELSE index_regexp
                   END,
                index_type =
                          CASE
                             WHEN p_index_type IS NULL
                                THEN index_type
                             WHEN p_index_type = 'NULL'
                                THEN NULL
                             ELSE index_type
                          END,
                partition_type =
                    CASE
                       WHEN p_part_type IS NULL
                          THEN partition_type
                       WHEN p_part_type = 'NULL'
                          THEN NULL
                       ELSE partition_type
                    END,
                constraint_regexp =
                   CASE
                      WHEN p_constraint_regexp IS NULL
                         THEN constraint_regexp
                      WHEN p_constraint_regexp = 'NULL'
                         THEN NULL
                      ELSE constraint_regexp
                   END,
                constraint_type =
                   CASE
                      WHEN p_constraint_type IS NULL
                         THEN constraint_type
                      WHEN p_constraint_type = 'NULL'
                         THEN NULL
                      ELSE constraint_type
                   END,
                description =
                      CASE
                         WHEN p_description IS NULL
                            THEN description
                         WHEN p_description = 'NULL'
                            THEN NULL
                         ELSE description
                      END,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE mapping_name = LOWER( p_mapping );
      END IF;

      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         evolve_log.log_msg( 'Updating configuration', 5 );

         -- try to update an existing configuration
         UPDATE mapping_conf
            SET mapping_name = LOWER( NVL( p_mapping, mapping_name )),
                mapping_type = LOWER( NVL( p_mapping_type, mapping_type )),
                table_owner = UPPER( NVL( p_owner, table_owner )),
                table_name = UPPER( NVL( p_table, table_name )),
                partition_name = UPPER( NVL( p_partname, partition_name )),
                manage_indexes = LOWER( NVL( p_indexes, manage_indexes )),
                manage_constraints = LOWER( NVL( p_constraints, manage_constraints )),
                source_owner = UPPER( NVL( p_source_owner, source_owner )),
                source_object = UPPER( NVL( p_source_object, source_object )),
                source_column = UPPER( NVL( p_source_column, source_column )),
                replace_method = LOWER( NVL( p_replace_method, replace_method )),
                STATISTICS = LOWER( NVL( p_statistics, STATISTICS )),
                concurrent = LOWER( NVL( p_concurrent, concurrent )),
                index_regexp = NVL( p_index_regexp, index_regexp ),
                index_type = NVL( p_index_type, index_type ),
                partition_type = NVL( p_part_type, partition_type ),
                constraint_regexp = NVL( p_constraint_regexp, constraint_regexp ),
                constraint_type = NVL( p_constraint_type, constraint_type ),
                description = NVL( p_description, description ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE mapping_name = LOWER( p_mapping );

         -- get the SQL rowcount
         l_num_rows    := SQL%ROWCOUNT;
      END IF;

      -- updating a current config has failed, or an insert was specified
      -- in this case, insert a new record
      IF ( l_num_rows = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         evolve_log.log_msg( 'Inserting configuration', 5 );

         CASE
            WHEN p_mapping IS NULL
            THEN
               evolve_log.raise_err( 'parm_req', 'P_MAPPING' );
            ELSE
               NULL;
         END CASE;

         BEGIN
            INSERT INTO mapping_conf
                        ( mapping_name, mapping_type, table_owner, table_name,
                          partition_name, manage_indexes, manage_constraints,
                          source_owner, source_object, source_column,
                          replace_method, STATISTICS,
                          concurrent, index_regexp, index_type, partition_type,
                          constraint_regexp, constraint_type, description
                        )
                 VALUES ( LOWER( p_mapping ), LOWER( p_mapping_type ), UPPER( p_owner ), UPPER( p_table ),
                          UPPER( p_partname ), LOWER( NVL( p_indexes, 'no' )), LOWER( NVL( p_constraints, 'no' )),
                          UPPER( p_source_owner ), UPPER( p_source_object ), UPPER( p_source_column ),
                          p_replace_method, LOWER( NVL( p_statistics, 'transfer' )),
                          LOWER( NVL( p_concurrent, 'no' )), p_index_regexp, p_index_type, p_part_type,
                          p_constraint_regexp, p_constraint_type, p_description
                        );

            -- get the SQL rowcount
            l_num_rows    := SQL%ROWCOUNT;
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve_log.raise_err( 'dup_conf' );
         END;
      END IF;

      IF LOWER( p_mode ) = 'delete'
      THEN
         -- if a delete is specifically requested, then do a delete
         DELETE FROM mapping_conf
               WHERE mapping_name = LOWER( p_mapping );

         -- get the SQL rowcount
         l_num_rows    := SQL%ROWCOUNT;
      ELSE
         -- as long as P_MODE wasn't 'delete', then we should validate the new structure of the dimension
         -- now use the dimension object to validate the new structure
         -- just constructing the object calls the CONFIRM_OBJECTS procedure
         BEGIN
            o_map    := trans_factory.get_mapping_ot( p_mapping => p_mapping );
         END;
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF l_num_rows = 0
      THEN
         evolve_log.raise_err( 'no_rep_obj' );
      END IF;
   END configure_mapping;

   PROCEDURE configure_mapping(
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
      p_description         VARCHAR2 DEFAULT NULL,
      p_mode                VARCHAR2 DEFAULT 'upsert'
   )
   IS
      l_map_type   mapping_conf.mapping_type%TYPE;
   BEGIN
      BEGIN
         --first, check to make sure that we should be modifying this record
         SELECT mapping_type
           INTO l_map_type
           FROM mapping_conf
          WHERE LOWER( mapping_name ) = LOWER( p_mapping );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record, then that's fine... this is a new mapping and all is well
            NULL;
      END;

      -- if this is a dimensional mapping, we should not modify it.
      IF LOWER( l_map_type ) = 'dimension'
      THEN
         evolve_log.raise_err( 'dim_map_obj' );
      END IF;

      -- now just configure the mapping
      configure_mapping( p_mapping                => p_mapping,
                         p_mapping_type           => 'table',
                         p_owner                  => p_owner,
                         p_table                  => p_table,
                         p_partname               => p_partname,
                         p_indexes                => p_indexes,
                         p_constraints            => p_constraints,
                         p_source_owner           => p_source_owner,
                         p_source_object          => p_source_object,
                         p_source_column          => p_source_column,
                         p_replace_method         => p_replace_method,
                         p_statistics             => p_statistics,
                         p_concurrent             => p_concurrent,
                         p_index_regexp           => p_index_regexp,
                         p_index_type             => p_index_type,
                         p_part_type              => p_part_type,
                         p_constraint_regexp      => p_constraint_regexp,
                         p_constraint_type        => p_constraint_type,
                         p_description            => p_description,
                         p_mode                   => p_mode
                       );
   END configure_mapping;

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
      p_stage_key_def      NUMBER DEFAULT NULL,
      p_char_nvl_def       VARCHAR2 DEFAULT NULL,
      p_date_nvl_def       DATE DEFAULT NULL,
      p_num_nvl_def        NUMBER DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   )
   IS
      o_dim        mapping_ot;
      l_mapping    mapping_conf.mapping_name%TYPE   := p_owner || '.' || p_table || ' load';
      l_num_rows   NUMBER;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      IF LOWER( p_mode ) = 'nullify'
      THEN
         evolve_log.log_msg( 'Nullifying configuration elements', 5 );

         UPDATE dimension_conf
            SET sequence_owner =
                   UPPER( CASE
                             WHEN p_sequence_owner IS NULL
                                THEN sequence_owner
                             WHEN p_sequence_owner = 'NULL'
                                THEN NULL
                             ELSE sequence_owner
                          END
                        ),
                sequence_name =
                   UPPER( CASE
                             WHEN p_sequence_name IS NULL
                                THEN sequence_name
                             WHEN p_sequence_name = 'NULL'
                                THEN NULL
                             ELSE sequence_name
                          END
                        ),
                staging_owner =
                   UPPER( CASE
                             WHEN p_staging_owner IS NULL
                                THEN staging_owner
                             WHEN p_staging_owner = 'NULL'
                                THEN NULL
                             ELSE staging_owner
                          END
                        ),
                staging_table =
                   UPPER( CASE
                             WHEN p_staging_table IS NULL
                                THEN staging_table
                             WHEN p_staging_table = 'NULL'
                                THEN NULL
                             ELSE staging_table
                          END
                        ),
                default_scd_type =
                   CASE
                      WHEN p_default_scd_type IS NULL
                         THEN default_scd_type
                      WHEN p_default_scd_type = 'NULL'
                         THEN NULL
                      ELSE default_scd_type
                   END,
                direct_load =
                   LOWER( CASE
                             WHEN p_direct_load IS NULL
                                THEN direct_load
                             WHEN p_direct_load = 'NULL'
                                THEN NULL
                             ELSE direct_load
                          END
                        ),
                stage_key_default =
                   CASE
                      WHEN p_stage_key_def IS NULL
                         THEN stage_key_default
                      WHEN p_stage_key_def = 'NULL'
                         THEN NULL
                      ELSE stage_key_default
                   END,
                char_nvl_default =
                   CASE
                      WHEN p_char_nvl_def IS NULL
                         THEN char_nvl_default
                      WHEN p_char_nvl_def = 'NULL'
                         THEN NULL
                      ELSE char_nvl_default
                   END,
                date_nvl_default =
                   CASE
                      WHEN p_date_nvl_def IS NULL
                         THEN date_nvl_default
                      WHEN p_date_nvl_def = 'NULL'
                         THEN NULL
                      ELSE date_nvl_default
                   END,
                number_nvl_default =
                   CASE
                      WHEN p_num_nvl_def IS NULL
                         THEN number_nvl_default
                      WHEN p_num_nvl_def = 'NULL'
                         THEN NULL
                      ELSE number_nvl_default
                   END,
                description =
                      CASE
                         WHEN p_description IS NULL
                            THEN description
                         WHEN p_description = 'NULL'
                            THEN NULL
                         ELSE description
                      END,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

         l_num_rows    := SQL%ROWCOUNT;
      END IF;

      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         evolve_log.log_msg( 'Updating configuration', 5 );

         -- first try to update an existing configuration
         UPDATE dimension_conf
            SET sequence_owner = UPPER( NVL( p_sequence_owner, sequence_owner )),
                sequence_name = UPPER( NVL( p_sequence_name, sequence_name )),
                staging_owner = UPPER( NVL( p_staging_owner, staging_owner )),
                staging_table = UPPER( NVL( p_staging_table, staging_table )),
                default_scd_type = NVL( p_default_scd_type, default_scd_type ),
                direct_load = LOWER( NVL( p_direct_load, direct_load )),
                stage_key_default = NVL( p_stage_key_def, stage_key_default ),
                char_nvl_default = NVL( p_char_nvl_def, char_nvl_default ),
                date_nvl_default = NVL( p_date_nvl_def, date_nvl_default ),
                number_nvl_default = NVL( p_num_nvl_def, number_nvl_default ),
                description = NVL( p_description, description ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

         -- get the SQL rowcount
         l_num_rows    := SQL%ROWCOUNT;
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
                        ( table_owner, table_name, sequence_owner, sequence_name,
                          staging_owner, staging_table, default_scd_type,
                          direct_load, stage_key_default,
                          char_nvl_default, date_nvl_default,
                          number_nvl_default, description
                        )
                 VALUES ( UPPER( p_owner ), UPPER( p_table ), UPPER( p_sequence_owner ), UPPER( p_sequence_name ),
                          UPPER( p_staging_owner ), UPPER( p_staging_table ), NVL( p_default_scd_type, 2 ),
                          LOWER( NVL( p_direct_load, 'yes' )), NVL( p_stage_key_def, -.01 ),
                          NVL( p_char_nvl_def, '~' ), NVL( p_date_nvl_def, TO_DATE( '01/01/9999', 'mm/dd/yyyy' )),
                          NVL( p_num_nvl_def, -.01 ), p_description
                        );

            -- get the SQL rowcount
            l_num_rows    := SQL%ROWCOUNT;
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve_log.raise_err( 'dup_conf' );
         END;
      END IF;

      IF LOWER( p_mode ) = 'delete'
      THEN
         -- if a delete is specifically requested, then do a delete
         DELETE FROM column_conf
               WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

         DELETE FROM dimension_conf
               WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

         -- get the SQL rowcount
         l_num_rows    := SQL%ROWCOUNT;
      END IF;

      -- now make the call to configure the mapping
      configure_mapping( p_mode                => p_mode,
                         p_mapping             => l_mapping,
                         p_mapping_type        => 'dimension',
                         p_table               => p_table,
                         p_owner               => p_owner,
                         p_source_owner        => p_source_owner,
                         p_source_object       => p_source_object,
                         p_replace_method      => p_replace_method,
                         p_statistics          => p_statistics,
                         p_concurrent          => p_concurrent
                       );

      IF LOWER( p_mode ) <> 'delete'
      THEN
         -- as long as P_MODE wasn't 'delete', then we should validate the new structure of the dimension
         -- now use the dimension object to validate the new structure
         -- just constructing the object calls the VERIFY procedure
         o_dim    := trans_factory.get_mapping_ot( l_mapping );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF l_num_rows = 0
      THEN
         evolve_log.raise_err( 'no_rep_obj' );
      END IF;
   END configure_dim;

   PROCEDURE configure_dim_cols(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_surrogate       VARCHAR2 DEFAULT NULL,
      p_nat_key         VARCHAR2 DEFAULT NULL,
      p_scd1            VARCHAR2 DEFAULT NULL,
      p_scd2            VARCHAR2 DEFAULT NULL,
      p_effective_dt    VARCHAR2 DEFAULT NULL,
      p_expiration_dt   VARCHAR2 DEFAULT NULL,
      p_current_ind     VARCHAR2 DEFAULT NULL
   )
   IS
      l_mapping    mapping_conf.mapping_name%TYPE   := p_owner || '.' || p_table || ' load';
      l_results    NUMBER;
      l_col_list   LONG;
      -- a dimension table should have already been configured
      o_dim        mapping_ot;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- construct a DIMENSION_OT object
      -- this is done using the supertype MAPPING_OT
      o_dim         := trans_factory.get_mapping_ot( l_mapping );
      -- construct the list for instrumentation purposes
      l_col_list    :=
         UPPER( td_core.format_list(    p_surrogate
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
      evolve_log.log_msg( 'The column list: ' || l_col_list, 5 );

      -- check and make sure all the columns specified are legitimate
      FOR c_cols IN ( SELECT COLUMN_VALUE column_name
                       FROM TABLE( CAST( td_core.SPLIT( l_col_list, ',' ) AS split_ot )))
      LOOP
         td_utils.check_column( p_owner => p_owner, p_table => p_table, p_column => c_cols.column_name );
      END LOOP;

      -- do the first merge to update any changed column_types from the parameters
      MERGE INTO column_conf t
         USING ( SELECT *
                  FROM ( SELECT owner, table_name, column_name, 'surrogate key' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( p_surrogate )
                        UNION
                        SELECT owner, table_name, column_name, 'effective date' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( p_effective_dt )
                        UNION
                        SELECT owner, table_name, column_name, 'expiration date' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( p_expiration_dt )
                        UNION
                        SELECT owner, table_name, column_name, 'current indicator' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( p_current_ind )
                        UNION
                        SELECT owner, table_name, column_name, 'natural key' column_type
                          FROM all_tab_columns atc JOIN TABLE( CAST( td_core.SPLIT( UPPER( p_nat_key ), ',' ) AS split_ot )
                                                             ) s ON atc.column_name = s.COLUMN_VALUE
                        UNION
                        SELECT owner, table_name, column_name, 'scd type 1' column_type
                          FROM all_tab_columns atc JOIN TABLE( CAST( td_core.SPLIT( UPPER( p_scd1 ), ',' ) AS split_ot )) s
                               ON atc.column_name = s.COLUMN_VALUE
                        UNION
                        SELECT owner, table_name, column_name, 'scd type 2' column_type
                          FROM all_tab_columns atc JOIN TABLE( CAST( td_core.SPLIT( UPPER( p_scd2 ), ',' ) AS split_ot )) s
                               ON atc.column_name = s.COLUMN_VALUE
                               )
                 WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table )) s
         ON (t.table_owner = s.owner AND t.table_name = s.table_name AND t.column_name = s.column_name )
         WHEN MATCHED THEN
            UPDATE
               SET t.column_type = s.column_type, t.modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                   t.modified_dt = SYSDATE
               WHERE s.column_type <> t.column_type
         WHEN NOT MATCHED THEN
            INSERT( t.table_owner, t.table_name, t.column_name, t.column_type )
            VALUES( s.owner, s.table_name, s.column_name, s.column_type );
      -- do the second merge to write any columns that have been left off
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
      -- confirm the dimension columns
      o_dim.confirm_dim_cols;
   END configure_dim_cols;
END trans_adm;
/

SHOW errors