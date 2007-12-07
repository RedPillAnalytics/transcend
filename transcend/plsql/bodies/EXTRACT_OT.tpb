CREATE OR REPLACE TYPE BODY extract_ot
AS
   -- extract data to a text file, and then peform other functions as defined in the configuration table
   MEMBER PROCEDURE process
   AS
      l_num_bytes   NUMBER;
      l_numlines    NUMBER;
      l_blocksize   NUMBER;
      l_exists      BOOLEAN                    DEFAULT FALSE;
      l_file_dt     DATE;
      l_detail_id   NUMBER;
      l_message     notification_events.MESSAGE%TYPE;
      l_results     NUMBER;
      o_ev          evolve_ot                  := evolve_ot( p_module => 'process' );
   BEGIN
      evolve_log.log_msg( 'Processing extract "' || file_label || '"',3 );
      o_ev.change_action( 'Configure NLS formats' );
      -- set date and timestamp NLS formats
      l_results :=
             evolve_app.exec_sql( p_sql      => dateformat_ddl,
                              p_msg      => 'nls_date_format DDL: ' );
      l_results :=
          evolve_app.exec_sql( p_sql      => tsformat_ddl,
                           p_msg      => 'nls_timestamp_format DDL: ' );
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
                       || arch_filepath, 3
                     );
      l_file_dt := SYSDATE;
      -- copy the file to the target location
      td_utils.copy_file( p_srcfile => arch_filepath, p_dstfile => filepath );
      evolve_log.log_msg(    'Archive file '
                       || arch_filepath
                       || ' copied to destination '
                       || filepath, 3
                     );

      -- get file attributes
      IF evolve_log.is_debugmode
      THEN
         l_num_bytes := 0;
         evolve_log.log_msg( 'Reporting 0 size file in debug mode',3 );
      ELSE
         UTL_FILE.fgetattr( DIRECTORY, filename, l_exists, l_num_bytes, l_blocksize );
      END IF;

      -- audit the file
      o_ev.change_action( 'Audit extract file' );
      SELF.audit_file( p_num_bytes      => l_num_bytes,
                       p_num_lines      => l_numlines,
                       p_file_dt        => l_file_dt
                     );

      -- notify about successful arrival of feed
      o_ev.change_action( 'Notify success' );
      announce_file( p_files_url => file_url );

      o_ev.clear_app_info;
   END process;
END;
/

SHOW errors
