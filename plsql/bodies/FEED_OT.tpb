CREATE OR REPLACE TYPE BODY feed_ot
AS
-- audits information about external tables after the file(s) have been put in place
   MEMBER PROCEDURE audit_ext_tab( p_num_lines NUMBER )
   IS
      l_num_rows         NUMBER          := 0;
      l_pct_miss         NUMBER;
      l_sql              VARCHAR2( 100 );
      e_data_cartridge   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_data_cartridge, -29913 );
      e_no_table         EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_table, -942 );
      e_no_files         EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_files, -1756 );
      o_ev               evolve_ot       := evolve_ot( p_module => 'audit_ext_tab' );
   BEGIN
      -- type object which handles logging and application registration for instrumentation purposes
      -- defaults to registering with DBMS_APPLICATION_INFO
      o_ev.change_action( 'Get count from table' );
      l_sql := 'SELECT count(*) FROM ' || SELF.object_owner || '.' || SELF.object_name;
      td_inst.log_msg( 'Count SQL: ' || l_sql, 3 );

      IF NOT td_inst.is_debugmode
      THEN
         BEGIN
            EXECUTE IMMEDIATE l_sql
                         INTO l_num_rows;
         EXCEPTION
            WHEN e_data_cartridge
            THEN
               -- use a regular expression to pull the KUP error out of SQLERRM
               CASE REGEXP_SUBSTR( SQLERRM, '^KUP-[[:digit:]]{5}', 1, 1, 'im' )
                  WHEN 'KUP-04040'
                  THEN
                     o_ev.change_action( 'location file missing' );
                     o_ev.send( p_label => self.file_label );
                     raise_application_error
                                           ( td_inst.get_err_cd( 'location_file_missing' ),
                                             td_inst.get_err_msg( 'location_file_missing' )
                                           );
                  ELSE
                     td_inst.log_msg( 'Unknown data cartridge error' );
               END CASE;
         END;

         BEGIN
            -- calculate the percentage difference
            l_pct_miss := 100 -( ( l_num_rows / p_num_lines ) * 100 );

            IF l_pct_miss > reject_limit
            THEN
               o_ev.change_action( 'reject limit exceeded' );
               -- notify if reject limit is exceeded
               o_ev.send( p_label => self.file_label );
               raise_application_error( td_inst.get_err_cd( 'reject_limit_exceeded' ),
                                        td_inst.get_err_msg( 'reject_limit_exceeded' )
                                      );
            END IF;
         EXCEPTION
            WHEN ZERO_DIVIDE
            THEN
               td_inst.log_msg( 'External table location is an empty file' );
         END;

         INSERT INTO files_obj_detail
                     ( file_obj_detail_id, file_type, file_label,
                       file_group, object_owner, object_name, num_rows,
                       num_lines, percent_diff
                     )
              VALUES ( files_obj_detail_seq.NEXTVAL, SELF.file_type, SELF.file_label,
                       SELF.file_group, SELF.object_owner, SELF.object_name, l_num_rows,
                       p_num_lines, l_pct_miss
                     );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN e_no_table
      THEN
         raise_application_error( td_inst.get_err_cd( 'no_tab' ),
                                     td_inst.get_err_msg( 'no_tab' )
                                  || ': '
                                  || SELF.object_owner
                                  || '.'
                                  || SELF.object_name
                                );
   END audit_ext_tab;
   MEMBER PROCEDURE process
   IS
      l_rows_dirlist   BOOLEAN                    := FALSE;     -- TO catch empty cursors
      l_rows_delete    BOOLEAN                    := FALSE;
      l_numlines       NUMBER;
      l_cmd            VARCHAR2( 500 );
      l_filepath       VARCHAR2( 200 );
      l_numfiles       NUMBER;
      l_sum_numlines   NUMBER                     := 0;
      l_ext_file_cnt   NUMBER;
      l_ext_tab_ddl    VARCHAR2( 2000 );
      l_files_url      VARCHAR2( 1000 );
      l_message        notification_events.MESSAGE1%TYPE;
      l_results        NUMBER;
      e_no_files       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_files, -1756 );
      o_ev             evolve_ot                  := evolve_ot( p_module => 'process' );
   BEGIN
      td_inst.log_msg( 'Processing feed "' || file_label || '"' );
      o_ev.change_action( 'Evaluate source directory' );

      -- first we remove all current files in the external table
      -- we don't want the possiblity of data for a previous run getting loaded in
      -- later, if no new files are found, and the REQUIRED attribute is 'N' (meaning this file is not required)
      -- then we will create an empty file
      FOR c_location IN ( SELECT  DIRECTORY, LOCATION
                             FROM dba_external_locations
                            WHERE owner = UPPER( object_owner )
                              AND table_name = UPPER( object_name )
                         ORDER BY LOCATION )
      LOOP
         l_rows_delete := TRUE;
         td_host.delete_file( c_location.DIRECTORY, c_location.LOCATION );
      END LOOP;

      IF l_rows_delete
      THEN
         td_inst.log_msg( 'Previous external table location files removed' );
      END IF;

      -- now we need to see all the source files in the source directory that match the regular expression
      -- use java stored procedure to populate global temp table DIR_LIST with all the files in the directory
      td_host.get_dir_list( source_dirpath );

      -- look at the contents of the DIR_LIST table to evaluate source files
      -- pull out only the ones matching the regular expression
      -- also work in a lot of the attributes to generate all the information needed for the object
      FOR c_dir_list IN
         ( SELECT  object_name,                                    -- external table owner
                               object_owner,                        -- external table name
                                            source_filename,  -- name of each source files
                                                            source_filepath,
                   
                   -- name converted to absolute path
                   CASE
                      -- use analytics to determine how many files are going into place
                      -- that tells us whether to increment the filenames
                   WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                         THEN REGEXP_REPLACE( filepath,
                                              '\.',
                                              '_' || file_number || '.'
                                            )
                      WHEN ext_tab_ind = 'N'
                         THEN NULL
                      ELSE filepath
                   END filepath,
                   CASE
                      WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                         THEN REGEXP_REPLACE( SELF.filename,
                                              '\.',
                                              '_' || file_number || '.'
                                            )
                      WHEN ext_tab_ind = 'N'
                         THEN NULL
                      ELSE SELF.filename
                   END filename,
                   pre_mv_filepath, arch_filepath, file_dt, file_size, ext_tab_ind,
                   ext_tab_type_cnt,
                      
                      -- use analytics (stragg function) to construct the alter table command (if needed)
                      'alter table '
                   || SELF.object_owner
                   || '.'
                   || SELF.object_name
                   || ' location ('
                   || REGEXP_REPLACE
                         ( STRAGG
                              (    SELF.DIRECTORY
                                || ':'''
                                || CASE
                                      WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                                         THEN REGEXP_REPLACE( SELF.filename,
                                                              '\.',
                                                              '_' || file_number || '.'
                                                            )
                                      WHEN ext_tab_ind = 'N'
                                         THEN NULL
                                      ELSE SELF.filename
                                   END
                              ) OVER( PARTITION BY ext_tab_ind ),
                           ',',
                           ''','
                         )
                   || ''')' alt_ddl,
                   
                             -- construct a file_url if BASEURL attribute is configured
                   -- this constructs a STRAGGED list of URL's if multiple files exist
                             -- otherwise it's null
                   REGEXP_REPLACE
                      ( STRAGG
                            (    SELF.baseurl
                              || '/'
                              || CASE
                                    WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                                       THEN REGEXP_REPLACE( SELF.filename,
                                                            '\.',
                                                            '_' || file_number || '.'
                                                          )
                                    WHEN ext_tab_ind = 'N'
                                       THEN NULL
                                    ELSE SELF.filename
                                 END
                            ) OVER( PARTITION BY ext_tab_ind ),
                        ',',
                        CHR( 10 )
                      ) files_url
              FROM ( SELECT object_name, object_owner, source_filename, source_filepath,
                            filepath, pre_mv_filepath, arch_filepath, file_dt, file_size,
                            ext_tab_ind,
                            
                            -- rank gives us a number to use to auto increment files in case SOURCE_POLICY attribute is 'all'
                            RANK( ) OVER( PARTITION BY 1 ORDER BY ext_tab_ind DESC,
                             source_filename ) file_number,
                            
                                           -- this gives us a count of how many files will be copied into the external table
                                           -- have this for each line
                            -- use the EXT_TAB_IND derived in the select below
                            COUNT( * ) OVER( PARTITION BY ext_tab_ind ) ext_tab_type_cnt
                      FROM ( SELECT            -- the dir_list table has a filename column
                                   -- we also have a filename attribute
                                   -- rename the filename from the table as SOURCE_FILENAME
                                   filename source_filename,
                                                            -- URL location if the target location is web enabled
                                                            -- this is for notification purposes to send links for received files
                                                            SELF.baseurl baseurl,
                                   
                                   -- translate directory objects and filenames to absolute paths
                                   -- because Java actually does most the heavy lifting
                                   -- and java doesn't know anything about a directory object
                                   -- path of the target object (if there's only one)
                                   SELF.filepath filepath,
                                   
                                   -- path to the source file
                                   SELF.source_dirpath || '/' || filename source_filepath,
                                   
                                   -- path to an intermediate file location just prior to being placed in the external table
                                   -- this is so the file can be decompressed and decrypted AFTER the move
                                   SELF.dirpath || '/' || filename pre_mv_filepath,
                                   
                                   -- use the attribute FILE_DATESTAMP to determin if the archived file needs a date added
                                   -- some files will come in dated already, and two datestamps are silly
                                   -- when we have a whole catalog to track the files
                                   CASE file_datestamp
                                      WHEN NULL
                                         THEN    SELF.arch_dirpath
                                              || '/'
                                              || filename
                                      ELSE    SELF.arch_dirpath
                                           || '/'
                                           || filename
                                           || '.'
                                           || TO_CHAR( SYSDATE, file_datestamp )
                                   END arch_filepath,
                                   file_dt, file_size,
                                   
                                   -- case statement determines an EXT_TAB_IND
                                   -- this picks out the files that will go to the external table
                                   -- uses the SOURCE_POLICY column to determine which ones to get
                                   -- that is translated to a Y/N indicator based on the date of the file
                                   CASE
                                      WHEN LOWER( SELF.source_policy ) =
                                                                     'newest'
                                      AND file_dt = MIN( file_dt ) OVER( PARTITION BY 1 )
                                         THEN 'Y'
                                      WHEN LOWER( SELF.source_policy ) = 'oldest'
                                      AND file_dt = MAX( file_dt ) OVER( PARTITION BY 1 )
                                         THEN 'Y'
                                      WHEN LOWER( SELF.source_policy ) = 'all'
                                         THEN 'Y'
                                      ELSE 'N'
                                   END ext_tab_ind,
                                   UPPER( SELF.object_name ) object_name,
                                   UPPER( SELF.object_owner ) object_owner
                              FROM dir_list
                             -- matching regexp and regexp_options to find matching source files
                            WHERE  REGEXP_LIKE( filename,
                                                SELF.source_regexp,
                                                SELF.regexp_options
                                              )))
          ORDER BY ext_tab_ind ASC )
      LOOP
         -- catch empty cursor sets
         l_rows_dirlist := TRUE;
         -- reset variables used in the cursor
         l_numlines := 0;
         -- copy file to the archive location
         o_ev.change_action( 'Copy archivefile' );
         td_host.copy_file( c_dir_list.source_filepath, c_dir_list.arch_filepath );
         td_inst.log_msg( 'Archive file ' || c_dir_list.arch_filepath || ' created' );
         -- copy the file to the external table
         o_ev.change_action( 'Copy external table files' );

         IF c_dir_list.ext_tab_ind = 'Y'
         THEN
            -- get the DDL to alter the external table after the loop is complete
                 -- this statement will be the same no matter which of the rows we pull it from.
                 -- might as well use the last
            l_ext_tab_ddl := c_dir_list.alt_ddl;
            -- record the number of external table files
            l_ext_file_cnt := c_dir_list.ext_tab_type_cnt;
            -- record the files url
            l_files_url := c_dir_list.files_url;
            -- first move the file to the target destination without changing the name
            -- because the file might be zipped or encrypted
            td_host.copy_file( c_dir_list.arch_filepath, c_dir_list.pre_mv_filepath );
            -- decrypt the file if it's encrypted
            -- currently only supports gpg
            -- decrypt_file will return the decrypted filename
            -- IF the file isn't a recognized encrypted file type, it just returns the name passed
            l_filepath :=
               td_host.decrypt_file( dirpath, c_dir_list.source_filename,
                                     SELF.passphrase );
            -- unzip the file if it's zipped
            -- currently will unzip, or gunzip, or bunzip2 or uncompress
            -- unzip_file will return the unzipped filename
            -- IF the file isn't a recognized zip archive file, it just returns the name passed
            l_filepath := td_host.unzip_file( dirpath, c_dir_list.source_filename );
                 -- now move the file to the expected name
            -- do this with a copy/delete
            td_host.copy_file( l_filepath, c_dir_list.filepath );
            td_host.delete_file( DIRECTORY, l_filepath );
            td_inst.log_msg(    'Source file '
                             || c_dir_list.source_filepath
                             || ' moved to destination '
                             || c_dir_list.filepath
                           );
            -- get the number of lines in the file now that it is decrypted and uncompressed
            l_numlines := td_host.get_numlines( SELF.DIRECTORY, c_dir_list.filename );
            -- get a total count of all the lines in all the files making up the external table
            l_sum_numlines := l_sum_numlines + l_numlines;
         END IF;

         -- WRITE an audit record for the file that was just archived
         IF NOT td_inst.is_debugmode
         THEN
            o_ev.change_action( 'Audit feed' );
            SELF.audit_file( p_source_filepath      => c_dir_list.source_filepath,
                             p_arch_filepath        => c_dir_list.arch_filepath,
                             p_filepath             => c_dir_list.filepath,
                             p_num_bytes            => c_dir_list.file_size,
                             p_num_lines            => l_numlines,
                             p_file_dt              => c_dir_list.file_dt
                           );
         END IF;

         -- IF we get this far, then we need to delete the source files
         -- this step is ignored if delete_source = 'no'
         o_ev.change_action( 'Delete source files' );

         IF td_ext.is_true( delete_source )
         THEN
            td_host.delete_file( source_directory, c_dir_list.source_filename );
         END IF;
      END LOOP;

      -- check to see if the cursor was empty
      o_ev.change_action( 'Check for matching files' );

      CASE
         WHEN NOT l_rows_dirlist AND required = 'Y'
         THEN
            raise_application_error( td_inst.get_err_cd( 'no_files_found' ),
                                     td_inst.get_err_msg( 'no_files_found' )
                                   );
         -- there were no matching files for this configuration
         -- however, the REQUIRED attribute is N
         -- therefore, and load process dependent on this job will proceed
         -- but need a "business logic" way of saying "no rows for today"
         -- so I empty the file out
         -- an external table with a zero-byte file gives "no rows returned"
      WHEN NOT l_rows_dirlist AND required = 'N'
         THEN
            td_inst.log_msg( 'No files found... but none are required' );
            o_ev.change_action( 'Empty previous files' );

            FOR c_location IN ( SELECT DIRECTORY, LOCATION
                                 FROM dba_external_locations
                                WHERE owner = UPPER( object_owner )
                                  AND table_name = UPPER( object_name ))
            LOOP
               td_host.create_file( c_location.DIRECTORY, c_location.LOCATION );
            END LOOP;
         WHEN l_rows_dirlist AND LOWER( source_policy ) = 'all'
         -- matching files found, so ignore
                  -- alter the external table to contain all the files
      THEN
            o_ev.change_action( 'Alter external table' );

            BEGIN
               l_results := td_sql.exec_sql( p_sql => l_ext_tab_ddl, p_auto => 'yes' );
               td_inst.log_msg(    'External table '
                                || object_owner
                                || '.'
                                || object_name
                                || ' altered'
                              );
            EXCEPTION
               WHEN e_no_files
               THEN
                  raise_application_error( td_inst.get_err_cd( 'no_ext_files' ),
                                           td_inst.get_err_msg( 'no_ext_files' )
                                         );
            END;

            -- audit the external table
            o_ev.change_action( 'Audit external table' );
            SELF.audit_ext_tab( p_num_lines => l_sum_numlines );
      END CASE;

      -- notify about successful arrival of feed
      o_ev.change_action( 'Notify success' );
      l_message :=
            'The file'
         || CASE
               WHEN l_ext_file_cnt > 1
                  THEN 's'
               ELSE NULL
            END
         || ' can be downloaded at the following link'
         || CASE
               WHEN l_ext_file_cnt > 1
                  THEN 's'
               ELSE NULL
            END
         || ':'
         || CHR( 10 )
         || l_files_url;

      IF l_numlines > 65536
      THEN
         l_message :=
               l_message
            || CHR( 10 )
            || CHR( 10 )
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      o_ev.send( p_label => self.file_label );
      o_ev.clear_app_info;
   END process;
END;
/