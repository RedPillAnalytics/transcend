CREATE OR REPLACE TYPE BODY feed_ot
AS
   CONSTRUCTOR FUNCTION feed_ot( p_file_group VARCHAR2, p_file_label VARCHAR2 )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- now load the other attributes
	 SELECT file_label, file_group, file_type, object_owner, object_name, DIRECTORY, dirpath, filename,
		dirpath || '/' || filename filepath, arch_directory, arch_dirpath, NULL arch_filename,
		NULL arch_filepath, file_datestamp, min_bytes, max_bytes, baseurl, NULL file_url, passphrase,
		source_directory, source_dirpath, source_regexp, match_parameter, source_policy, required,
		delete_source, reject_limit
           INTO file_label, file_group, file_type, object_owner, object_name, DIRECTORY, dirpath, filename,
		filepath, arch_directory, arch_dirpath, arch_filename,
		arch_filepath, file_datestamp, min_bytes, max_bytes, baseurl, file_url, passphrase,
		source_directory, source_dirpath, source_regexp, match_parameter, source_policy, required,
		delete_source, reject_limit
           FROM ( SELECT file_label, file_group, file_type, object_owner, object_name, DIRECTORY,
			 td_utils.get_dir_path( DIRECTORY ) dirpath, filename, arch_directory,
			 td_utils.get_dir_path( arch_directory ) arch_dirpath, file_datestamp, min_bytes,
			 max_bytes, baseurl, passphrase, source_directory,
			 td_utils.get_dir_path( source_directory ) source_dirpath, source_regexp, match_parameter,
			 source_policy, required, delete_source, reject_limit
                    FROM files_conf
		   WHERE REGEXP_LIKE( file_type, '^feed$', 'i' )
                     AND file_group = p_file_group
                     AND file_label = p_file_label );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_feed', p_file_label );
      END;

      
      verify;

      RETURN;
   END feed_ot;

   MEMBER PROCEDURE verify
   IS
      l_dir_path    all_directories.directory_path%TYPE;
      l_directory   all_external_tables.default_directory_name%TYPE;
      o_ev   evolve_ot := evolve_ot( p_module => 'verify' );
   BEGIN
      -- do checks to make sure all the provided information is legitimate
      -- check to see if the directories are legitimate
      -- if they aren't, the GET_DIR_PATH function raises an error
      l_dir_path := td_utils.get_dir_path( self.arch_directory );
      l_dir_path := td_utils.get_dir_path( self.source_directory );
      l_dir_path := td_utils.get_dir_path( self.directory );
      
      -- make sure the external table exists
      td_utils.check_table ( p_owner         => SELF.object_owner,
                             p_table         => SELF.object_name,
                             p_external      => 'yes'
                           );

      -- now need to find out what the directory is associated with the external table
      BEGIN
	 -- get the directory from the external table
         SELECT default_directory_name
           INTO l_directory
           FROM all_external_tables
          WHERE owner = self.object_owner AND table_name = self.object_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
         evolve.raise_err( 'no_ext_tab', UPPER( self.object_owner || '.' || self.object_name ));
      END;
      
      -- now compare the two and make sure they are the same
      IF upper(self.directory) <> l_directory
      THEN
	 evolve.raise_err( 'no_dir_match' );
      END IF;

      evolve.log_msg( 'FEED confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   -- audits information about external tables after the file(s) have been put in place
   MEMBER PROCEDURE audit_ext_tab( p_num_lines NUMBER )
   IS
      l_num_rows         NUMBER          := 0;
      l_pct_miss         NUMBER;
      l_sql              VARCHAR2( 100 );
      l_ext_tab          VARCHAR2( 61 )  := SELF.object_owner || '.' || SELF.object_name;
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
      o_ev.change_action( 'get count from table' );
      l_sql := 'SELECT count(*) FROM ' || l_ext_tab;
      evolve.log_msg( 'Count SQL: ' || l_sql, 3 );
      o_ev.change_action( 'get external table count' );

      IF NOT evolve.is_debugmode
      THEN
         BEGIN
            EXECUTE IMMEDIATE l_sql
                         INTO l_num_rows;
         EXCEPTION
            WHEN e_data_cartridge
            THEN
               -- no matter what happens, we want to log the error
               -- this is prior to the case on purpose
               evolve.log_err;

                    -- use a regular expression to pull the KUP error out of SQLERRM
               -- this tells us the explicit issue with the external table
               CASE REGEXP_SUBSTR( SQLERRM, '^KUP-[[:digit:]]{5}', 1, 1, 'im' )
                       -- so far, only one known error to check for
                  -- others will come
               WHEN 'KUP-04040'
                  THEN
                     o_ev.change_action( 'external file missing' );
                     o_ev.send( p_label => SELF.file_label );
                     o_ev.clear_app_info;
                     evolve.raise_err( 'ext_file_missing', l_ext_tab );
                  -- All other errors get routed here
               ELSE
                     o_ev.clear_app_info;
                     evolve.raise_err( 'data_cartridge', l_ext_tab );
               END CASE;
         END;

         BEGIN
            -- calculate the percentage difference
            l_pct_miss := 100 -( ( l_num_rows / p_num_lines ) * 100 );

            IF l_pct_miss > reject_limit
            THEN
               o_ev.change_action( 'reject limit exceeded' );
               -- notify if reject limit is exceeded
               o_ev.send( p_label => SELF.file_label );
               o_ev.clear_app_info;
               evolve.raise_err( 'reject_limit_exceeded' );
            END IF;
         EXCEPTION
            WHEN ZERO_DIVIDE
            THEN
               evolve.log_msg( 'External table location is an empty file', 3 );
         END;

         INSERT INTO files_obj_detail
                     ( file_obj_detail_id, file_type, file_label, file_group,
                       object_owner, object_name, num_rows, num_lines, percent_diff
                     )
              VALUES ( files_obj_detail_seq.NEXTVAL, SELF.file_type, SELF.file_label, SELF.file_group,
                       SELF.object_owner, SELF.object_name, l_num_rows, p_num_lines, l_pct_miss
                     );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN e_no_table
      THEN
         evolve.raise_err( 'no_tab', SELF.object_owner || '.' || SELF.object_name );
   END audit_ext_tab;
   MEMBER PROCEDURE process
   IS
      l_rows_dirlist   BOOLEAN                            := FALSE;                 -- TO catch empty cursors
      l_rows_delete    BOOLEAN                            := FALSE;
      l_numlines       NUMBER;
      l_max_numlines   NUMBER                             := 0;
      l_cmd            VARCHAR2( 500 );
      l_filepath       VARCHAR2( 200 );
      l_numfiles       NUMBER;
      l_sum_numlines   NUMBER                             := 0;
      l_ext_file_cnt   NUMBER                             := 0;
      l_ext_tab_ddl    VARCHAR2( 2000 );
      l_ext_tab        VARCHAR2( 61 )                     := object_owner || '.' || object_name;
      l_files_url      VARCHAR2( 1000 );
      l_message        notification_events.MESSAGE%TYPE;
      l_results        NUMBER;
      e_no_files       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_files, -1756 );
      o_ev             evolve_ot                          := evolve_ot( p_module => 'process' );
   BEGIN
      evolve.log_msg( 'Processing feed "' || file_label || '"', 3 );
      o_ev.change_action( 'evaluate source directory' );
      -- we are about to copy files into place
      -- before that, we need to remove files placed in on previous runs
      -- we don't want the possiblity of data for a previous run getting loaded in
      -- later, if no new files are found, and the REQUIRED attribute is 'N' (meaning this file is not required)
      -- THEN we will create an empty file

      o_ev.change_action( 'delete current locations' );

      FOR c_location IN ( SELECT  DIRECTORY, LOCATION
                            FROM dba_external_locations
                           WHERE owner = UPPER( object_owner ) AND table_name = UPPER( object_name )
                           ORDER BY LOCATION )
      LOOP
         l_rows_delete := TRUE;
         td_utils.delete_file( c_location.DIRECTORY, c_location.LOCATION );
      END LOOP;

      IF l_rows_delete
      THEN
         evolve.log_msg( 'Previous external table location files removed', 3 );
      END IF;
      

      -- now we need to see all the source files in the source directory that match the regular expression
      -- USE java stored procedure to populate global temp table DIR_LIST with all the files in the directory
      td_utils.get_dir_list( source_dirpath );

      -- look at the contents of the DIR_LIST table to evaluate source files
      -- pull out only the ones matching the regular expression
      -- also work in a lot of the attributes to generate all the information needed for the object
      FOR c_dir_list IN
         ( SELECT  object_name,                                                        -- external table owner
                               object_owner,                                            -- external table name
                                            source_filename,                      -- name of each source files
                                                            source_filepath,
                   
                   -- name converted to absolute path
                   CASE
                      -- USE analytics to determine how many files are going into place
                      -- that tells us whether to increment the filenames
                   WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                         THEN REGEXP_REPLACE( filepath, '\.', '_' || file_number || '.' )
                      WHEN ext_tab_ind = 'N'
                         THEN NULL
                      ELSE filepath
                   END filepath,
                   CASE
                      WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                         THEN REGEXP_REPLACE( SELF.filename, '\.', '_' || file_number || '.' )
                      WHEN ext_tab_ind = 'N'
                         THEN NULL
                      ELSE SELF.filename
                   END filename,
                   pre_mv_filepath, arch_filepath, file_dt, file_size, ext_tab_ind, ext_tab_type_cnt,
                      
                      -- USE analytics (stragg function) to construct the alter table command (if needed)
                      'alter table '
                   || l_ext_tab
                   || ' location ('
                   || REGEXP_REPLACE
                         ( STRAGG(    SELF.DIRECTORY
                                || ':'''
                                || CASE
                                      WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                                         THEN REGEXP_REPLACE( SELF.filename, '\.', '_' || file_number || '.' )
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
                      ( STRAGG(    SELF.baseurl
                             || '/'
                             || CASE
                                   WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                                      THEN REGEXP_REPLACE( SELF.filename, '\.', '_' || file_number || '.' )
                                   WHEN ext_tab_ind = 'N'
                                      THEN NULL
                                   ELSE SELF.filename
                                END
                           ) OVER( PARTITION BY ext_tab_ind ),
                        ',',
                        CHR( 10 )
                      ) files_url
              FROM ( SELECT object_name, object_owner, source_filename, source_filepath, filepath,
                            pre_mv_filepath, arch_filepath, file_dt, file_size, ext_tab_ind,
                            
                            -- rank gives us a number to use to auto increment files in case SOURCE_POLICY attribute is 'all'
                            RANK( ) OVER( PARTITION BY 1 ORDER BY ext_tab_ind DESC,
                             source_filename ) file_number,
                            
                            -- this gives us a count of how many files will be copied into the external table
                            -- have this for each line
                            -- USE the EXT_TAB_IND derived in the select below
                            COUNT( * ) OVER( PARTITION BY ext_tab_ind ) ext_tab_type_cnt
                      FROM ( SELECT                                -- the dir_list table has a filename column
                                   -- we also have a filename attribute
                                   -- RENAME the filename from the table as SOURCE_FILENAME
                                   filename source_filename,
                                                            -- URL location if the target location is web enabled
                                                            -- this is for notification purposes to send links for received files
                                                            SELF.baseurl baseurl,
                                                                                 -- translate directory objects and filenames to absolute paths
                                                                                 -- because Java actually does most the heavy lifting
                                                                                 -- AND java doesn't know anything about a directory object
                                                                                 -- path of the target object (if there's only one)
                                                                                 SELF.filepath filepath,
                                   
                                   -- path to the source file
                                   SELF.source_dirpath || '/' || filename source_filepath,
                                   
                                   -- path to an intermediate file location just prior to being placed in the external table
                                   -- this is so the file can be decompressed and decrypted AFTER the move
                                   SELF.dirpath || '/' || filename pre_mv_filepath,
                                   
                                   -- USE the attribute FILE_DATESTAMP to determin if the archived file needs a date added
                                   -- SOME files will come in dated already, and two datestamps are silly
                                   -- WHEN we have a whole catalog to track the files
                                   CASE
                                      WHEN file_datestamp IS NULL
                                         THEN SELF.arch_dirpath || '/' || filename
                                      ELSE    SELF.arch_dirpath
                                           || '/'
                                           || filename
                                           || '.'
                                           || TO_CHAR( SYSDATE, file_datestamp )
                                   END arch_filepath,
                                   file_dt, file_size,
                                   
                                   -- CASE statement determines an EXT_TAB_IND
                                   -- this picks out the files that will go to the external table
                                   -- uses the SOURCE_POLICY column to determine which ones to get
                                   -- that is translated to a Y/N indicator based on the date of the file
                                   CASE
                                      WHEN LOWER( SELF.source_policy ) = 'newest'
                                      AND file_dt = MAX( file_dt ) OVER( PARTITION BY 1 )
                                         THEN 'Y'
                                      WHEN LOWER( SELF.source_policy ) = 'oldest'
                                      AND file_dt = MIN( file_dt ) OVER( PARTITION BY 1 )
                                         THEN 'Y'
                                      WHEN LOWER( SELF.source_policy ) = 'all'
                                         THEN 'Y'
                                      ELSE 'N'
                                   END ext_tab_ind,
                                   UPPER( SELF.object_name ) object_name,
                                   UPPER( SELF.object_owner ) object_owner
                              FROM dir_list
                             -- matching regexp and match_parameter to find matching source files
                            WHERE  REGEXP_LIKE( filename, SELF.source_regexp, SELF.match_parameter )))
          ORDER BY ext_tab_ind ASC )
      LOOP
         o_ev.change_action( 'process feed' );
         evolve.log_msg( 'Processing file ' || c_dir_list.source_filepath, 3 );
         -- catch empty cursor sets
         l_rows_dirlist := TRUE;
         -- reset variables used in the cursor
         l_numlines := 0;
         -- copy file to the archive location
         o_ev.change_action( 'copy archivefile' );
         td_utils.copy_file( c_dir_list.source_filepath, c_dir_list.arch_filepath );
         evolve.log_msg( 'Archive file ' || c_dir_list.arch_filepath || ' created', 3 );
         -- copy the file to the external table
         o_ev.change_action( 'copy external table files' );

         IF c_dir_list.ext_tab_ind = 'Y'
         THEN
            -- get the DDL to alter the external table after the loop is complete
            -- this statement will be the same no matter which of the rows we pull it from.
            -- might as well use the last
            l_ext_tab_ddl := c_dir_list.alt_ddl;
            -- RECORD the number of external table files
            l_ext_file_cnt := c_dir_list.ext_tab_type_cnt;
            -- RECORD the files url
            l_files_url := c_dir_list.files_url;
            -- first move the file to the target destination without changing the name
            -- because the file might be zipped or encrypted
            td_utils.copy_file( c_dir_list.arch_filepath, c_dir_list.pre_mv_filepath );
            -- decrypt the file if it's encrypted
            -- currently only supports gpg
            -- decrypt_file will return the decrypted filename
            -- IF the file isn't a recognized encrypted file type, it just returns the name passed
            l_filepath := td_utils.decrypt_file( dirpath, c_dir_list.source_filename, SELF.passphrase );
            -- unzip the file if it's zipped
            -- currently will unzip, or gunzip, or bunzip2 or uncompress
            -- unzip_file will return the unzipped filename
            -- IF the file isn't a recognized zip archive file, it just returns the name passed
            l_filepath := td_utils.unzip_file( dirpath, c_dir_list.source_filename );

            -- now move the file to the expected name
            -- do this with a copy/delete
            IF dirpath || '/' || l_filepath <> c_dir_list.filepath
            THEN
               td_utils.copy_file( dirpath || '/' || l_filepath, c_dir_list.filepath );
               td_utils.delete_file( DIRECTORY, l_filepath );
               evolve.log_msg(    'Source file '
                                   || c_dir_list.source_filepath
                                   || ' moved to destination '
                                   || c_dir_list.filepath
                                 );
            END IF;

            -- get the number of lines in the file now that it is decrypted and uncompressed
            l_numlines := td_utils.get_numlines( SELF.DIRECTORY, c_dir_list.filename );
            -- get a total count of all the lines in all the files making up the external table
            l_sum_numlines := l_sum_numlines + l_numlines;

            -- see if this is the maximum line number size
            -- if it is, then keep it
            IF l_numlines > l_max_numlines
            THEN
               l_max_numlines := l_numlines;
            END IF;
         END IF;

         -- WRITE an audit record for the file that was just archived
         IF NOT evolve.is_debugmode
         THEN
            o_ev.change_action( 'audit feed' );
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
         o_ev.change_action( 'delete source files' );

         IF td_core.is_true( delete_source )
         THEN
            td_utils.delete_file( source_directory, c_dir_list.source_filename );
         END IF;
      END LOOP;

      -- series of debug statements
      o_ev.change_action( 'check for matching files' );
      evolve.log_msg( 'Attribute REQUIRED is: ' || required, 5 );
      evolve.log_msg( 'Attribute SOURCE_POLICY is: ' || source_policy, 5 );
      evolve.log_msg( 'The number of files moved to target: ' || l_ext_file_cnt, 5 );
      evolve.log_msg( 'Variable L_ROWS_DIRLIST is: ' || CASE
                             WHEN l_rows_dirlist
                                THEN 'TRUE'
                             ELSE 'FALSE'
                          END, 5 );

      CASE
         -- there were no files found, and the file is required
         -- then we should fail
      WHEN NOT l_rows_dirlist AND td_core.is_true( required )
         THEN
            evolve.raise_err( 'no_files_found' );
         -- there were no files found
         -- however, the REQUIRED attribute is "no"
         -- therefore, any load process dependent on this job should proceed
         -- but need a "business logic" way of saying "no rows for today"
         -- so I empty the file out
         -- an external table with a zero-byte file gives "no rows returned"
      WHEN NOT l_rows_dirlist AND NOT td_core.is_true( required )
         THEN
            evolve.log_msg( 'No files found... but none are required', 3 );
            o_ev.change_action( 'empty previous files' );

            FOR c_location IN ( SELECT DIRECTORY, LOCATION
                                 FROM dba_external_locations
                                WHERE owner = UPPER( object_owner ) AND table_name = UPPER( object_name ))
            LOOP
               td_utils.create_file( c_location.DIRECTORY, c_location.LOCATION );
            END LOOP;
         WHEN l_rows_dirlist AND l_ext_file_cnt > 0
         -- matching files found, and the number of location files is greater than zero
         -- alter the external table to contain all the files
      THEN
            o_ev.change_action( 'alter external table' );

            BEGIN
               l_results := evolve.exec_sql( p_sql => l_ext_tab_ddl, p_auto => 'yes' );
               evolve.log_msg( 'External table ' || l_ext_tab || ' altered', 3 );
            EXCEPTION
               WHEN e_no_files
               THEN
                  evolve.raise_err( 'no_ext_files', l_ext_tab );
            END;

            -- audit the external table
            o_ev.change_action( 'audit external table' );
            SELF.audit_ext_tab( p_num_lines => l_sum_numlines );
         WHEN l_rows_dirlist AND l_ext_file_cnt = 0
              -- matching files found, but there were no location files
              -- there were files found at the OS level
         -- however, no files deemed as possible locations
         -- the only explanation for this is that there are multiple files found
         -- and the SOURCE_POLICY is 'fail'
      THEN
            o_ev.change_action( 'fail source policy enacted' );
            evolve.raise_err( 'fail_source_policy' );
         ELSE
            NULL;
      END CASE;

      -- notify about successful arrival of feed
      o_ev.change_action( 'notify success' );
      SELF.announce_file( p_num_files      => l_ext_file_cnt,
                          p_num_lines      => l_max_numlines,
                          p_files_url      => l_files_url
                        );
      o_ev.clear_app_info;
   END process;
END;
/

SHOW errors