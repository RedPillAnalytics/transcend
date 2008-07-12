CREATE OR REPLACE TYPE BODY extract_ot
AS
   CONSTRUCTOR FUNCTION extract_ot( p_file_group VARCHAR2, p_file_label VARCHAR2 )
      RETURN SELF AS RESULT
   AS
   BEGIN
      SELECT file_label, file_group, file_type, object_owner, object_name, DIRECTORY, dirpath, filename,
             td_utils.get_dir_path( DIRECTORY ) || '/' || filename filepath, arch_directory, arch_dirpath,
             arch_filename, td_utils.get_dir_path( arch_directory ) || '/' || arch_filename arch_filepath,
             file_datestamp, min_bytes, max_bytes, baseurl,
             CASE baseurl
                WHEN NULL
                   THEN NULL
                ELSE baseurl || '/' || filename
             END file_url, passphrase,
             'alter session set nls_date_format=''' || DATEFORMAT || '''' dateformat_ddl,
             'alter session set nls_timestamp_format=''' || timestampformat || '''' tsformat_ddl, delimiter,
             quotechar, headers
        INTO file_label, file_group, file_type, object_owner, object_name, DIRECTORY, dirpath, filename,
             filepath, arch_directory, arch_dirpath,
             arch_filename, arch_filepath,
             file_datestamp, min_bytes, max_bytes, baseurl,
             file_url, passphrase,
             dateformat_ddl,
             tsformat_ddl, delimiter,
             quotechar, headers
        FROM ( SELECT file_label, file_group, file_type, object_owner, object_name, DIRECTORY,
                      td_utils.get_dir_path( DIRECTORY ) dirpath,
                      CASE NVL( file_datestamp, 'NA' )
                         WHEN 'NA'
                            THEN filename
                         ELSE REGEXP_REPLACE( filename,
                                              '\.',
                                              '_' || TO_CHAR( SYSDATE, file_datestamp ) || '.'
                                            )
                      END filename,
                      CASE NVL( file_datestamp, 'NA' )
                         WHEN 'NA'
                            THEN filename || '.' || TO_CHAR( SYSDATE, 'yyyymmddhhmiss' )
                         ELSE REGEXP_REPLACE( filename,
                                              '\.',
                                              '_' || TO_CHAR( SYSDATE, file_datestamp ) || '.'
                                            )
                      END arch_filename,
                      arch_directory, td_utils.get_dir_path( arch_directory ) arch_dirpath, file_datestamp,
                      min_bytes, max_bytes, baseurl, passphrase, DATEFORMAT, timestampformat, delimiter,
                      quotechar, headers
                FROM files_conf
               WHERE REGEXP_LIKE( file_type, '^extract$', 'i' )
                 AND file_group = p_file_group
                 AND file_label = p_file_label );
	     
	     -- verify that all the parameters are correct
	     verify;
      RETURN;
	  END extract_ot;
	  
   MEMBER PROCEDURE verify
   IS
      l_dir_path    all_directories.directory_path%TYPE;
      l_directory   all_external_tables.default_directory_name%TYPE;
      o_ev   evolve_ot := evolve_ot( p_module => 'confirm' );
   BEGIN
      -- check to see if the directories are legitimate
      -- if they aren't, the GET_DIR_PATH function raises an error
      l_dir_path := td_utils.get_dir_path( self.arch_directory );
      l_dir_path := td_utils.get_dir_path( self.directory );

      evolve_log.log_msg( 'FEED confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   -- extract data to a text file, and then peform other functions as defined in the configuration table
   MEMBER PROCEDURE process
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

      evolve_log.log_msg( 'Previous NLS_DATE_FORMAT of session: ' || l_curr_df, 5 );

      -- get current timestamp format
      SELECT VALUE
        INTO l_curr_tsf
        FROM nls_session_parameters
       WHERE parameter = 'NLS_TIMESTAMP_FORMAT';

      evolve_log.log_msg( 'Previous NLS_TIMESTAMP_FORMAT of session: ' || l_curr_df, 5 );
      evolve_log.log_msg( 'Processing extract "' || file_label || '"', 3 );
      o_ev.change_action( 'Configure NLS formats' );
      -- set date and timestamp NLS formats
      evolve_log.exec_sql( p_sql => dateformat_ddl, p_msg => 'nls_date_format DDL: ' );
      evolve_log.exec_sql( p_sql => tsformat_ddl, p_msg => 'nls_timestamp_format DDL: ' );
      o_ev.change_action( 'Extract data' );
      -- extract data to arch location first
      l_numlines :=
         td_utils.extract_object( p_owner          => object_owner,
                                  p_object         => object_name,
                                  p_dirname        => arch_directory,
                                  p_filename       => arch_filename,
                                  p_delimiter      => delimiter,
                                  p_quotechar      => quotechar,
                                  p_headers        => headers
                                );
      evolve_log.log_msg(    l_numlines
                          || ' '
                          || CASE l_numlines
                                WHEN 1
                                   THEN 'row'
                                ELSE 'rows'
                             END
                          || ' extracted to '
                          || arch_filepath,
                          3
                        );
      l_file_dt := SYSDATE;
      -- copy the file to the target location
      td_utils.copy_file( p_srcfile => arch_filepath, p_dstfile => filepath );
      evolve_log.log_msg( 'Archive file ' || arch_filepath || ' copied to destination ' || filepath, 3 );

      -- get file attributes
      IF evolve_log.is_debugmode
      THEN
         l_num_bytes := 0;
         evolve_log.log_msg( 'Reporting 0 size file in debug mode', 3 );
      ELSE
         UTL_FILE.fgetattr( DIRECTORY, filename, l_exists, l_num_bytes, l_blocksize );

         IF NOT l_exists
         THEN
            evolve_log.raise_err( 'file_not_found', filename );
         END IF;
      END IF;

      -- audit the file
      o_ev.change_action( 'Audit extract file' );
      SELF.audit_file( p_num_bytes => l_num_bytes, p_num_lines => l_numlines, p_file_dt => l_file_dt );
      -- log general information message
      evolve_log.log_msg( 'Extract file ' || filepath || ' generated' );
      -- notify about successful arrival of feed
      -- only works if this notification event has been configured for the file label.
      o_ev.change_action( 'Notify success' );
      SELF.announce_file( p_files_url => file_url, p_num_lines => l_numlines );
      -- set date and timestamp NLS formats back to original
      evolve_log.exec_sql( p_sql      => 'alter session set nls_date_format=''' || l_curr_df || '''',
                           p_msg      => 'nls_date_format DDL: '
                         );
      evolve_log.exec_sql( p_sql      => 'alter session set nls_timestamp_format=''' || l_curr_tsf || '''',
                           p_msg      => 'nls_timestamp_format DDL: '
                         );
      o_ev.clear_app_info;
   END process;
END;
/

SHOW errors