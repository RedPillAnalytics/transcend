CREATE OR REPLACE PACKAGE BODY td_utils
AS
   -- procedure executes the host_cmd function and raises an exception with the return code
   PROCEDURE host_cmd( p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ' )
   AS
      l_retval   NUMBER;
      o_ev       evolve_ot := evolve_ot( p_module => 'host_cmd' );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT evolve_log.is_debugmode
      THEN
         l_retval := host_cmd( p_cmd, p_stdin );

         IF l_retval <> 0
         THEN
            raise_application_error
                            ( -20020,
                              'Java Error: method hostCmd made unsuccessful system calls'
                            );
         END IF;
      END IF;

      evolve_log.log_msg( 'Host command: ' || p_cmd, 3 );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END host_cmd;

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE copy_file( p_srcfile VARCHAR2, p_dstfile VARCHAR2 )
   AS
      l_retval   NUMBER;
      o_ev       evolve_ot := evolve_ot( p_module => 'copy_file' );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT evolve_log.is_debugmode
      THEN
         l_retval := copy_file( p_srcfile, p_dstfile );

         IF l_retval <> 0
         THEN
            raise_application_error
                            ( -20020,
                                 'Java Error: method TdCore.copyFile was unable to copy '
                              || p_srcfile
                              || ' to '
                              || p_dstfile
                            );
         END IF;
      END IF;

      evolve_log.log_msg( 'File ' || p_srcfile || ' copied to ' || p_dstfile, 3 );
      o_ev.clear_app_info;
   END copy_file;

   -- uses UTL_FILE to remove an OS level file
   PROCEDURE delete_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_retval     NUMBER;
      l_filepath   VARCHAR2( 100 );
      o_ev         evolve_ot          := evolve_ot( p_module => 'delete_file' );
   BEGIN
      l_filepath := td_utils.get_dir_path( p_directory ) || '/' || p_filename;

      IF NOT evolve_log.is_debugmode
      THEN
         UTL_FILE.fremove( p_directory, p_filename );
      END IF;

      evolve_log.log_msg( 'File ' || l_filepath || ' deleted', 3 );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         evolve_log.log_msg( l_filepath || ' could not be deleted, or does not exist' );
   END delete_file;

   -- uses UTL_FILE to "touch" a file
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_fh        UTL_FILE.file_type;
      l_dirpath   VARCHAR2( 100 );
      o_ev        evolve_ot             := evolve_ot( p_module => 'create_file' );
   BEGIN
      l_dirpath := td_utils.get_dir_path( p_directory ) || '/' || p_filename;

      IF NOT evolve_log.is_debugmode
      THEN
         l_fh := UTL_FILE.fopen( p_directory, p_filename, 'W' );
      END IF;

      evolve_log.log_msg( 'File ' || l_dirpath || ' created', 3 );
      o_ev.clear_app_info;
   END create_file;

   -- get the number of lines in a file
   FUNCTION get_numlines( p_dirname IN VARCHAR2, p_filename IN VARCHAR2 )
      RETURN NUMBER
   AS
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2( 2000 );
      l_cnt    NUMBER             := 0;
      o_ev     evolve_ot             := evolve_ot( p_module => 'get_numlines' );
   BEGIN
      IF evolve_log.is_debugmode
      THEN
         evolve_log.log_msg( td_inst.module || ' returning 0 because of DEBUG mode' );
         o_ev.clear_app_info;
         RETURN 0;
      ELSE
         BEGIN
            l_fh := UTL_FILE.fopen( p_dirname, p_filename, 'R', 32767 );

            LOOP
               UTL_FILE.get_line( l_fh, l_line );
               l_cnt := l_cnt + 1;
            END LOOP;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               UTL_FILE.fclose( l_fh );
               o_ev.clear_app_info;
               RETURN l_cnt;
         END;
      END IF;
   END get_numlines;

   -- a function used to unzip a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- function returns what the name should be after the unzip process
   FUNCTION unzip_file( p_dirpath VARCHAR2, p_filename VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_compressed     BOOLEAN         := TRUE;
      l_filebase       VARCHAR2( 50 );
      l_filesuf        VARCHAR2( 20 );
      l_filepath       VARCHAR2( 200 ) := p_dirpath || '/' || p_filename;
      l_filebasepath   VARCHAR2( 200 );
      l_cmd            VARCHAR2( 200 );
      l_return         VARCHAR2( 200 );
      l_file_exists    BOOLEAN;
      l_file_size      NUMBER;
      l_blocksize      NUMBER;
      o_ev             evolve_ot          := evolve_ot( p_module => 'unzip_file' );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename, '\.[^\.]+$', NULL, 1, 1, 'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;
      evolve_log.log_msg( l_filepath || ' checked for compression using standard libraries',
                       3
                     );

      CASE l_filesuf
         WHEN 'gz'
         THEN
            host_cmd( 'gzip -df ' || l_filepath );
            evolve_log.log_msg( l_filepath || ' gunzipped', 3 );
         WHEN 'Z'
         THEN
            host_cmd( 'uncompress ' || l_filepath );
            evolve_log.log_msg( l_filepath || ' uncompressed', 3 );
         WHEN 'bz2'
         THEN
            host_cmd( 'bunzip2 ' || l_filepath );
            evolve_log.log_msg( l_filepath || ' bunzipped', 3 );
         WHEN 'zip'
         THEN
            host_cmd( 'unzip ' || l_filepath );
            evolve_log.log_msg( l_filepath || ' unzipped', 3 );
         ELSE
            -- this is the only case where the file wasn't compressed
            l_compressed := FALSE;
      END CASE;

      -- return either the expected uncompressed filename
      -- or the provided filename if no unzip process was performed
      IF l_compressed
      THEN
         l_return := l_filebasepath;
      ELSE
         l_return := l_filepath;
      END IF;

      IF evolve_log.is_debugmode
      THEN
         evolve_log.log_msg( 'File returned by UNZIP_FILE: ' || l_return );
      ELSE
         o_ev.change_action( 'Check for extracted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( td_utils.get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize
                          );

         IF NOT l_file_exists
         THEN
            raise_application_error( td_inst.get_err_cd( 'file_not_found' ),
                                     td_inst.get_err_msg( 'file_not_found' )
                                   );
         END IF;
      END IF;

      o_ev.clear_app_info;
      RETURN l_return;
   END unzip_file;

   -- a function used to decrypt a file regardless of which method was used to encrypt it
   -- currently contains functionality for the following encryption methods: gpg
   -- function returns what the name should be after the decryption process
   FUNCTION decrypt_file( p_dirpath VARCHAR2, p_filename VARCHAR2, p_passphrase VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_encrypted      BOOLEAN         := TRUE;
      l_filebase       VARCHAR2( 50 );
      l_filesuf        VARCHAR2( 20 );
      l_filepath       VARCHAR2( 200 ) := p_dirpath || '/' || p_filename;
      l_filebasepath   VARCHAR2( 200 );
      l_cmd            VARCHAR2( 200 );
      l_return         VARCHAR2( 200 );
      l_file_exists    BOOLEAN;
      l_file_size      NUMBER;
      l_blocksize      NUMBER;
      o_ev             evolve_ot          := evolve_ot( p_module => 'decrypt_file' );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename, '\.[^\.]+$', NULL, 1, 1, 'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gpg'
         THEN
            host_cmd(    'gpg --no-tty --passphrase-fd 0 --batch --decrypt --output '
                      || l_filepath
                      || ' '
                      || l_filebasepath,
                      p_passphrase
                    );
         ELSE
            -- this is the only case where the extension wasn't recognized
            l_encrypted := FALSE;
      END CASE;

      -- return either the expected decrypted filename
      -- or the provided filename if the decryption process was performed
      IF l_encrypted
      THEN
         l_return := l_filebasepath;
      ELSE
         l_return := l_filepath;
      END IF;

      IF evolve_log.is_debugmode
      THEN
         evolve_log.log_msg( 'File returned by DECRYPT_FILE: ' || l_return );
      ELSE
         o_ev.change_action( 'Check for decrypted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( td_utils.get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize
                          );

         IF NOT l_file_exists
         THEN
            raise_application_error( -20020, 'Filename to return does not exist' );
         END IF;
      END IF;

      o_ev.clear_app_info;
      RETURN l_return;
   END decrypt_file;

   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   AS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2( 2000 );
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2( 5 )      DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
      l_mode          VARCHAR2( 1 )      := CASE LOWER( p_append )
         WHEN 'yes'
            THEN 'a'
         ELSE 'w'
      END;
      l_exists        BOOLEAN;
      l_length        NUMBER;
      l_blocksize     NUMBER;
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_var, -1007 );
      o_ev            evolve_ot             := evolve_ot( p_module => 'extract_query' );
   BEGIN
      l_output := UTL_FILE.fopen( p_dirname, p_filename, l_mode, 32767 );
      DBMS_SQL.parse( l_thecursor, p_query, DBMS_SQL.native );
      o_ev.change_action( 'Open Cursor to define columns' );

      FOR i IN 1 .. 255
      LOOP
         BEGIN
            DBMS_SQL.define_column( l_thecursor, i, l_columnvalue, 2000 );
            l_colcnt := i;
         EXCEPTION
            WHEN e_no_var
            THEN
               EXIT;
         END;
      END LOOP;

      DBMS_SQL.define_column( l_thecursor, 1, l_columnvalue, 2000 );
      l_status := DBMS_SQL.EXECUTE( l_thecursor );
      o_ev.change_action( 'Open Cursor to pull back records' );

      LOOP
         EXIT WHEN( DBMS_SQL.fetch_rows( l_thecursor ) <= 0 );
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE( l_thecursor, i, l_columnvalue );

            IF NOT evolve_log.is_debugmode
            THEN
               UTL_FILE.put( l_output,
                             l_delimiter || p_quotechar || l_columnvalue || p_quotechar
                           );
            END IF;

            l_delimiter := p_delimiter;
         END LOOP;

         UTL_FILE.new_line( l_output );
         l_cnt := l_cnt + 1;
      END LOOP;

      o_ev.change_action( 'Close cursor and handles' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );
      o_ev.clear_app_info;
      RETURN l_cnt;
   END extract_query;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   FUNCTION extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   IS
      l_cnt           NUMBER           := 0;
      l_head_sql      VARCHAR( 1000 );
      l_extract_sql   VARCHAR2( 1000 );
      o_ev            evolve_ot           := evolve_ot( p_module => 'extract_object' );
   BEGIN
      -- check that the source object exists and is something we can select from
      td_utils.check_object( p_owner            => p_owner,
                           p_object           => p_object,
                           p_object_type      => 'table$|view'
                         );
      l_head_sql :=
            'select regexp_replace(stragg(column_name),'','','''
         || p_delimiter
         || ''') from '
         || '(select '''
         || p_quotechar
         || '''||column_name||'''
         || p_quotechar
         || ''' as column_name'
         || ' from all_tab_cols '
         || 'where table_name='''
         || UPPER( p_object )
         || ''' and owner='''
         || UPPER( p_owner )
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;
      evolve_log.log_msg( 'Headers query: ' || l_head_sql, 3 );
      evolve_log.log_msg( 'Extract query: ' || l_extract_sql, 3 );

      IF NOT evolve_log.is_debugmode
      THEN
         IF td_core.is_true( p_headers )
         THEN
            o_ev.change_action( 'Extract headers to file' );
            l_cnt :=
               extract_query( p_query          => l_head_sql,
                              p_dirname        => p_dirname,
                              p_filename       => p_filename,
                              p_delimiter      => p_delimiter,
                              p_quotechar      => NULL,
                              p_append         => p_append
                            );
         END IF;

         o_ev.change_action( 'Extract data to file' );
         l_cnt :=
              l_cnt
            + extract_query( p_query          => l_extract_sql,
                             p_dirname        => p_dirname,
                             p_filename       => p_filename,
                             p_delimiter      => p_delimiter,
                             p_quotechar      => p_quotechar,
                             p_append         => CASE
                                WHEN td_core.is_true( p_headers )
                                   THEN 'yes'
                                ELSE p_append
                             END
                           );
      END IF;

      o_ev.clear_app_info;
      RETURN l_cnt;
   END extract_object;

   -- this process is called by submitted jobs to DBMS_SCHEDULER
   -- when SQL is submitted through SUBMIT_SQL, this is what those submitted jobs actually call
   PROCEDURE consume_sql(
      p_session_id  NUMBER,
      p_module	    VARCHAR2,
      p_action	    VARCHAR2,
      p_sql         VARCHAR2,
      p_msg         VARCHAR2
   )
   AS
   BEGIN
      -- use the SET_SCHEDULER_SESSION_ID procedure to register with the framework
      -- this allows all logging entries to be kept together
      td_inst.set_scheduler_info( p_session_id => p_session_id,
				  p_module     => p_module,
				  p_action     => p_action );

      -- load session parameters configured in PARAMETER_CONF for this module
      -- this is usually done by EVOLVE_OT, but that is not applicable here
      FOR c_params IN
         ( SELECT CASE
                  WHEN REGEXP_LIKE( NAME, 'enable|disable', 'i' )
                  THEN 'alter session ' || NAME || ' ' || VALUE
                  ELSE 'alter session set ' || NAME || '=' || VALUE
                  END DDL
             FROM parameter_conf
            WHERE LOWER( module ) = td_inst.module )
      LOOP
         IF evolve_log.is_debugmode
         THEN
            evolve_log.log_msg( 'Session SQL: ' || c_params.DDL );
         ELSE
            EXECUTE IMMEDIATE ( c_params.DDL );
         END IF;
      END LOOP;
      
      -- just use the standard procedure to execute the SQL
      exec_sql( p_sql => p_sql,
		p_msg => p_msg );
      
   EXCEPTION
      WHEN others
      THEN 
      evolve_log.log_err;
      RAISE;
   END consume_sql;

END td_utils;
/