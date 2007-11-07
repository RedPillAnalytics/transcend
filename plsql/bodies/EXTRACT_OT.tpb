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
      l_message     notification_events.MESSAGE1%TYPE;
      l_results     NUMBER;
      o_ev          evolve_ot                  := evolve_ot( p_module => 'process' );
   BEGIN
      td_inst.log_msg( 'Processing extract "' || file_label || '"' );
      o_ev.change_action( 'Configure NLS formats' );
      -- set date and timestamp NLS formats
      l_results :=
             td_sql.exec_sql( p_sql      => dateformat_ddl,
                              p_msg      => 'nls_date_format DDL: ' );
      l_results :=
          td_sql.exec_sql( p_sql      => tsformat_ddl,
                           p_msg      => 'nls_timestamp_format DDL: ' );
      o_ev.change_action( 'Extract data' );
      -- extract data to arch location first
      l_numlines :=
         td_host.extract_object( p_owner          => object_owner,
                                 p_object         => object_name,
                                 p_dirname        => arch_directory,
                                 p_filename       => arch_filename,
                                 p_delimiter      => delimiter,
                                 p_quotechar      => quotechar,
                                 p_headers        => headers
                               );
      td_inst.log_msg(    l_numlines
                       || ' '
                       || CASE l_numlines
                             WHEN 1
                                THEN 'row'
                             ELSE 'rows'
                          END
                       || ' extracted to '
                       || arch_filepath
                     );
      l_file_dt := SYSDATE;
      -- copy the file to the target location
      td_host.copy_file( p_srcfile => arch_filepath, p_dstfile => filepath );
      td_inst.log_msg(    'Archive file '
                       || arch_filepath
                       || ' copied to destination '
                       || filepath
                     );

      -- get file attributes
      IF td_inst.is_debugmode
      THEN
         l_num_bytes := 0;
         td_inst.log_msg( 'Reporting 0 size file in debug mode' );
      ELSE
         UTL_FILE.fgetattr( DIRECTORY, filename, l_exists, l_num_bytes, l_blocksize );
      END IF;

      -- audit the file
      o_ev.change_action( 'Audit extract file' );
      SELF.audit_file( p_num_bytes      => l_num_bytes,
                       p_num_lines      => l_numlines,
                       p_file_dt        => l_file_dt
                     );
      -- send the notification if configured
      o_ev.change_action( 'Notify success' );
      l_message :=
               'The file can be downloaded at the following link:' || CHR( 10 )
               || file_url;

      IF l_numlines > 65536
      THEN
         l_message :=
               l_message
            || CHR( 10 )
            || CHR( 10 )
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      o_ev.send( p_label => file_label, p_message => l_message );
      o_ev.clear_app_info;
   END process;
END;
/