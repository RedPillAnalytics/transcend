CREATE OR REPLACE TYPE BODY extract_ot
AS
   CONSTRUCTOR FUNCTION extract_ot(
      p_file_label   VARCHAR2,
      p_directory    VARCHAR2 DEFAULT NULL 
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      SELECT file_label, file_group, label_type, object_owner, object_name, DIRECTORY, filename,
             CASE WHEN work_directory IS NOT NULL AND lower(work_directory) <> lower(directory)
		                THEN work_directory
		                ELSE NULL 
		            END work_directory,
	     file_datestamp, min_bytes, max_bytes,
             CASE baseurl
                WHEN NULL
                   THEN NULL
                ELSE baseurl || '/' || filename
             END file_url,
             'alter session set nls_date_format=''' || DATEFORMAT || '''' dateformat_ddl,
             'alter session set nls_timestamp_format=''' || timestampformat || '''' tsformat_ddl, delimiter,
             quotechar, headers
        INTO file_label, file_group, label_type, object_owner, object_name, directory, filename,
             work_directory, min_bytes, max_bytes, baseurl,
             file_url, dateformat_ddl,
             tsformat_ddl, delimiter,
             quotechar, headers
        FROM ( SELECT file_label, file_group, label_type, object_owner, object_name,
   		   NVL( p_directory, directory) directory,
                   CASE
                         WHEN file_datestamp IS null
                            THEN filename
                         ELSE REGEXP_REPLACE( filename,
                                              '\.',
                                              '_' || TO_CHAR( SYSDATE, file_datestamp ) || '.'
                                            )
                      END filename, work_directory,
                      min_bytes, max_bytes, baseurl, dateformat, timestampformat, delimiter,
                      quotechar, headers
                FROM file_conf
               WHERE REGEXP_LIKE( label_type, '^extract$', 'i' )
                 AND file_label = p_file_label );
	     
	     -- verify that all the parameters are correct
	     verify;
      RETURN;
   END extract_ot;
	  
   OVERRIDING MEMBER PROCEDURE verify
   IS
      l_dir_path    all_directories.directory_path%TYPE;
      l_directory   all_external_tables.default_directory_name%TYPE;
      o_ev   evolve_ot := evolve_ot( p_module => 'confirm' );
   BEGIN
      -- check to see if the directories are legitimate
      -- if they aren't, the GET_DIR_PATH function raises an error
      l_dir_path := td_utils.get_dir_path( self.directory );

      IF work_directory IS NOT NULL
      THEN
	 l_dir_path := td_utils.get_dir_path( self.work_directory );
      END IF;
      
      -- also, make sure that the work_directory and directory are not the same
      IF SELF.directory = SELF.work_directory
      THEN
	 evolve.raise_err ('parms_not_compatible','The values specified for DIRECTORY and WORK_DIRECTORY cannot be the same');
      END IF;

      evolve.log_msg( 'EXTRACT confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   -- extract data to a text file, and then peform other functions as defined in the configuration table
   OVERRIDING MEMBER PROCEDURE process
   AS
      l_num_bytes   NUMBER;
      l_numlines    NUMBER;
      l_blocksize   NUMBER;
      l_exists      BOOLEAN                             DEFAULT FALSE;
      l_file_dt     DATE;
      l_detail_id   NUMBER;
      l_message     notification_events.MESSAGE%TYPE;
      l_curr_df     nls_session_parameters.VALUE%TYPE;
      l_curr_tsf    nls_session_parameters.VALUE%TYPE;
      o_ev          evolve_ot                           := evolve_ot( p_module => 'process' );
   BEGIN
      -- get current date format
      SELECT VALUE
        INTO l_curr_df
        FROM nls_session_parameters
       WHERE parameter = 'NLS_DATE_FORMAT';

      evolve.log_msg( 'Previous NLS_DATE_FORMAT of session: ' || l_curr_df, 5 );

      -- get current timestamp format
      SELECT VALUE
        INTO l_curr_tsf
        FROM nls_session_parameters
       WHERE parameter = 'NLS_TIMESTAMP_FORMAT';

      evolve.log_msg( 'Previous NLS_TIMESTAMP_FORMAT of session: ' || l_curr_df, 5 );
      evolve.log_msg( 'Processing extract "' || file_label || '"', 3 );
      o_ev.change_action( 'Configure NLS formats' );
      -- set date and timestamp NLS formats
      evolve.exec_sql( p_sql => dateformat_ddl, p_msg => 'nls_date_format DDL: ' );
      evolve.exec_sql( p_sql => tsformat_ddl, p_msg => 'nls_timestamp_format DDL: ' );
      o_ev.change_action( 'Extract data' );
      -- extract data to arch location first
      l_numlines :=
         td_utils.extract_object( p_owner          => object_owner,
                                  p_object         => object_name,
                                  p_directory      => nvl( work_directory, directory),
                                  p_filename       => filename,
                                  p_delimiter      => delimiter,
                                  p_quotechar      => quotechar,
                                  p_headers        => headers
                                );

      evolve.log_msg(    l_numlines
                          || ' '
                          || CASE l_numlines
                                WHEN 1
                                   THEN 'row'
                                ELSE 'rows'
                             END
                          || ' extracted to file '
                          || self.filename
			  || ' in directory '
			  || nvl( work_directory, directory ),
                          3
                        );
      l_file_dt := SYSDATE;

      -- get file attributes
      IF evolve.is_debugmode
      THEN
         l_num_bytes := 0;
         evolve.log_msg( 'Reporting 0 size file in debug mode', 3 );
      ELSE
         UTL_FILE.fgetattr( DIRECTORY, filename, l_exists, l_num_bytes, l_blocksize );

         IF NOT l_exists
         THEN
            evolve.raise_err( 'file_not_found', filename );
         END IF;
      END IF;

      
      -- now we need to archive the file
      -- this writes important information about the file, as well as the file itself, the the database
      o_ev.change_action ( 'archive extract' );
      IF NOT evolve.is_debugmode
      THEN
	 -- this writes auditing information in the repository
	 -- also stores the file in the database
         SELF.archive ( p_num_bytes            => l_num_bytes,
			p_num_lines            => l_numlines,
			p_file_dt              => l_file_dt
                      );
      END IF;
      
      
      -- if there is a work_directory, then the file is there
      -- we need to move it to the target location
      IF self.work_directory IS NOT NULL
      THEN
	 td_utils.move_file( p_directory => self.directory, 
			     p_filename  => self.filename,
			     p_source_directory => self.work_directory,
			     p_source_filename  => self.filename );

	 evolve.log_msg( 'File '||self.filename||' moved from '||self.work_directory||' to '||self.directory );
      END IF;

      -- notify about successful arrival of feed
      -- only works if this notification event has been configured for the file label.
      o_ev.change_action( 'Notify success' );
      SELF.announce( p_files_url => file_url, p_num_lines => l_numlines );
      -- set date and timestamp NLS formats back to original
      evolve.exec_sql( p_sql      => 'alter session set nls_date_format=''' || l_curr_df || '''',
                           p_msg      => 'nls_date_format DDL: '
                         );
      evolve.exec_sql( p_sql      => 'alter session set nls_timestamp_format=''' || l_curr_tsf || '''',
                           p_msg      => 'nls_timestamp_format DDL: '
                         );
      o_ev.clear_app_info;
   END process;
END;
/

SHOW errors