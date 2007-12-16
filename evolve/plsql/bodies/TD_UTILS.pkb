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
            evolve_log.raise_err( 'host_cmd' );
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
            evolve_log.raise_err( 'copy_file' );
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
      o_ev         evolve_ot       := evolve_ot( p_module => 'delete_file' );
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
         evolve_log.log_msg( l_filepath || ' could not be deleted, or does not exist', 3 );
   END delete_file;

   -- uses UTL_FILE to "touch" a file
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_fh        UTL_FILE.file_type;
      l_dirpath   VARCHAR2( 100 );
      o_ev        evolve_ot          := evolve_ot( p_module => 'create_file' );
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
      o_ev     evolve_ot          := evolve_ot( p_module => 'get_numlines' );
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
      o_ev             evolve_ot       := evolve_ot( p_module => 'unzip_file' );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename, '\.[^\.]+$', NULL, 1, 1, 'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;
      evolve_log.log_msg( l_filepath || ' checked for compression using standard libraries', 3 );

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
         l_return := l_filebase;
      ELSE
         l_return := l_filebase || '.' || l_filesuf;
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
            evolve_log.raise_err( 'file_not_found' );
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
      o_ev             evolve_ot       := evolve_ot( p_module => 'decrypt_file' );
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
            evolve_log.raise_err( 'file_not_found', p_dirpath );
         END IF;
      END IF;

      o_ev.clear_app_info;
      RETURN l_return;
   END decrypt_file;

   -- checks things about a table depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_table(
      p_owner         VARCHAR2,
      p_table         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_iot           VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL,
      p_external      VARCHAR2 DEFAULT NULL
   )
   AS
      l_tab_name         VARCHAR2( 61 )                         := UPPER( p_owner ) || '.'
                                                                   || UPPER( p_table );
      l_part_name        VARCHAR2( 92 )                           := l_tab_name || ':' || UPPER( p_partname );
      l_partitioned      VARCHAR2( 3 );
      l_iot              VARCHAR2( 3 );
      l_compressed       VARCHAR2( 3 );
      l_partition_name   all_tab_partitions.partition_name%TYPE;
   BEGIN
      -- now get compression, partitioning and iot information
      BEGIN
         SELECT CASE
                   WHEN compression = 'DISABLED'
                      THEN 'no'
                   WHEN compression = 'N/A'
                      THEN 'no'
                   WHEN compression IS NULL
                      THEN 'no'
                   ELSE 'yes'
                END,
                LOWER( partitioned ) partitioned, CASE iot_type
                   WHEN 'IOT'
                      THEN 'yes'
                   ELSE 'no'
                END iot
           INTO l_compressed,
                l_partitioned, l_iot
           FROM all_tables
          WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_tab', l_tab_name );
      END;

      -- now just work through the gathered information and raise the appropriate exceptions.
      IF l_partitioned = 'yes' AND p_partname IS NULL AND p_compressed IS NOT NULL
      THEN
         evolve_log.raise_err( 'parms_not_compatible',
                               'P_COMPRESSED requires P_PARTNAME when the table is partitioned'
                             );
      END IF;

      IF p_partname IS NOT NULL
      THEN
         IF l_partitioned = 'no'
         THEN
            evolve_log.raise_err( 'not_partitioned', l_tab_name );
         END IF;

         BEGIN
            SELECT CASE
                      WHEN compression = 'DISABLED'
                         THEN 'no'
                      WHEN compression = 'N/A'
                         THEN 'no'
                      WHEN compression IS NULL
                         THEN 'no'
                      ELSE 'yes'
                   END
              INTO l_compressed
              FROM all_tab_partitions
             WHERE table_owner = UPPER( p_owner )
               AND table_name = UPPER( p_table )
               AND partition_name = UPPER( p_partname );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               evolve_log.raise_err( 'no_part', l_part_name );
         END;
      END IF;

      CASE
         WHEN td_core.is_true( p_partitioned, TRUE ) AND NOT td_core.is_true( l_partitioned )
         THEN
            evolve_log.raise_err( 'not_partitioned', l_tab_name );
         WHEN NOT td_core.is_true( p_partitioned, TRUE ) AND td_core.is_true( l_partitioned )
         THEN
            evolve_log.raise_err( 'partitioned', l_tab_name );
         WHEN     td_core.is_true( p_external, TRUE )
              AND NOT ext_table_exists( p_owner => p_owner, p_table => p_table )
         THEN
            evolve_log.raise_err( 'not_external', l_tab_name );
         WHEN     NOT td_core.is_true( p_external, TRUE )
              AND ext_table_exists( p_owner => p_owner, p_table => p_table )
         THEN
            evolve_log.raise_err( 'external', l_tab_name );
         WHEN td_core.is_true( p_iot, TRUE ) AND NOT td_core.is_true( l_iot )
         THEN
            evolve_log.raise_err( 'not_iot', l_tab_name );
         WHEN NOT td_core.is_true( p_iot, TRUE ) AND td_core.is_true( l_iot )
         THEN
            evolve_log.raise_err( 'iot', l_tab_name );
         WHEN td_core.is_true( p_compressed, TRUE ) AND NOT td_core.is_true( l_compressed )
         THEN
            evolve_log.raise_err( 'not_compressed', CASE
                                     WHEN p_partname IS NULL
                                        THEN l_tab_name
                                     ELSE l_part_name
                                  END );
         WHEN NOT td_core.is_true( p_compressed, TRUE ) AND td_core.is_true( l_compressed )
         THEN
            evolve_log.raise_err( 'compressed', CASE
                                     WHEN p_partname IS NULL
                                        THEN l_tab_name
                                     ELSE l_part_name
                                  END );
         ELSE
            NULL;
      END CASE;
   END check_table;

   -- checks things about an object depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_object( p_owner VARCHAR2, p_object VARCHAR2, p_object_type VARCHAR2 DEFAULT NULL )
   AS
      l_obj_name      VARCHAR2( 61 )                 := UPPER( p_owner ) || '.' || UPPER( p_object );
      l_object_name   all_objects.object_name%TYPE;
   BEGIN
      BEGIN
         SELECT DISTINCT object_name
                    INTO l_object_name
                    FROM all_objects
                   WHERE owner = UPPER( p_owner )
                     AND object_name = UPPER( p_object )
                     AND REGEXP_LIKE( object_type, NVL( p_object_type, '.' ), 'i' );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_or_wrong_object', l_obj_name );
         WHEN TOO_MANY_ROWS
         THEN
            evolve_log.raise_err( 'too_many_objects' );
      END;
   END check_object;

   -- used to get the path associated with a directory location
   FUNCTION get_dir_path( p_dirname VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER( p_dirname );

      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         evolve_log.raise_err( 'no_dir_obj', p_dirname );
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_dirname   all_directories.directory_name%TYPE;
   BEGIN
      SELECT directory_name
        INTO l_dirname
        FROM all_directories
       WHERE directory_path = p_dir_path;

      RETURN l_dirname;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         evolve_log.raise_err( 'no_dir_path', p_dir_path );
      WHEN TOO_MANY_ROWS
      THEN
         evolve_log.raise_err( 'too_many_dirs', p_dir_path );
   END get_dir_name;

   -- returns a boolean
   -- does a check to see if a table exists
   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_table   all_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM all_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END table_exists;

   -- returns a boolean
   -- does a check to see if an external table exists
   FUNCTION ext_table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_table   all_external_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM all_external_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END ext_table_exists;

   -- returns a boolean
   -- does a check to see if table is partitioned
   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_partitioned   all_tables.partitioned%TYPE;
   BEGIN
      IF NOT table_exists( UPPER( p_owner ), UPPER( p_table ))
      THEN
         evolve_log.raise_err( 'no_tab', p_owner || '.' || p_table );
      END IF;

      SELECT partitioned
        INTO l_partitioned
        FROM all_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      CASE
         WHEN td_core.is_true( l_partitioned )
         THEN
            RETURN TRUE;
         WHEN NOT td_core.is_true( l_partitioned )
         THEN
            RETURN FALSE;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END is_part_table;

   -- returns a boolean
   -- does a check to see if a object exists
   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_object   all_objects.object_name%TYPE;
   BEGIN
      SELECT DISTINCT object_name
                 INTO l_object
                 FROM all_objects
                WHERE owner = UPPER( p_owner ) AND object_name = UPPER( p_object );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END object_exists;

   -- called by the EXEC_SQL function when an autonomous transaction is desired
   -- and the number of results are desired
   FUNCTION exec_auto( p_sql VARCHAR2 )
      RETURN NUMBER
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_results   NUMBER;
   BEGIN
      EXECUTE IMMEDIATE p_sql;

      l_results := SQL%ROWCOUNT;
      COMMIT;
      RETURN l_results;
   END exec_auto;

   -- accepts the P_AUTO flag and determines whether to execute the statement
   -- if the P_AUTO flag of 'yes' is passed, then EXEC_AUTO is called
   FUNCTION exec_sql(
      p_sql              VARCHAR2,
      p_auto             VARCHAR2 DEFAULT 'no',
      p_msg              VARCHAR2 DEFAULT NULL,
      p_override_debug   VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   AS
      l_results   NUMBER;
   BEGIN
      IF NOT evolve_log.is_debugmode OR NOT td_core.is_true( p_override_debug )
      THEN
         IF td_core.is_true( p_auto )
         THEN
            l_results := exec_auto( p_sql => p_sql );
         ELSE
            EXECUTE IMMEDIATE p_sql;

            l_results := SQL%ROWCOUNT;
         END IF;
      END IF;

      RETURN l_results;
   END exec_sql;

   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT NULL,
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
      o_ev            evolve_ot          := evolve_ot( p_module => 'extract_query' );
   BEGIN
      l_output := UTL_FILE.fopen( p_dirname, p_filename, l_mode, 32767 );
      DBMS_SQL.parse( l_thecursor, p_query, DBMS_SQL.native );
      o_ev.change_action( 'define columns' );

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
      o_ev.change_action( 'extract records' );

      LOOP
         EXIT WHEN( DBMS_SQL.fetch_rows( l_thecursor ) <= 0 );
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE( l_thecursor, i, l_columnvalue );

            IF NOT evolve_log.is_debugmode
            THEN
               UTL_FILE.put( l_output, l_delimiter || p_quotechar || l_columnvalue || p_quotechar );
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
      p_quotechar   VARCHAR2 DEFAULT NULL,
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   IS
      l_cnt           NUMBER           := 0;
      l_head_sql      VARCHAR( 1000 );
      l_extract_sql   VARCHAR2( 1000 );
      o_ev            evolve_ot        := evolve_ot( p_module => 'extract_object' );
   BEGIN
      -- check that the source object exists and is something we can select from
      td_utils.check_object( p_owner => p_owner, p_object => p_object, p_object_type => 'table$|view' );
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
END td_utils;
/

SHOW errors