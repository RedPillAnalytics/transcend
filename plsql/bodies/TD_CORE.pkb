CREATE OR REPLACE PACKAGE BODY td_core
AS
   -- procedure executes the host_cmd function and raises an exception with the return code
   PROCEDURE host_cmd(
      p_cmd       VARCHAR2,
      p_stdin     VARCHAR2 DEFAULT ' ',
      p_runmode   VARCHAR2 DEFAULT NULL
   )
   AS
      l_retval   NUMBER;
      o_app      applog := applog( p_module => 'host_cmd', p_runmode => p_runmode );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT o_app.is_debugmode
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

      o_app.log_msg( 'Host command: ' || p_cmd, 3 );
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END host_cmd;

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE copy_file(
      p_srcfile   VARCHAR2,
      p_dstfile   VARCHAR2,
      p_runmode   VARCHAR2 DEFAULT NULL
   )
   AS
      l_retval   NUMBER;
      o_app      applog := applog( p_module => 'copy_file', p_runmode => p_runmode );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT o_app.is_debugmode
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

      o_app.log_msg( 'File ' || p_srcfile || ' copied to ' || p_dstfile, 3 );
      o_app.clear_app_info;
   END copy_file;

   -- uses UTL_FILE to remove an OS level file
   PROCEDURE delete_file(
      p_directory   VARCHAR2,
      p_filename    VARCHAR2,
      p_runmode     VARCHAR2 DEFAULT NULL
   )
   AS
      l_retval     NUMBER;
      l_filepath   VARCHAR2( 100 );
      o_app        applog  := applog( p_module       => 'delete_file',
                                      p_runmode      => p_runmode );
   BEGIN
      l_filepath := get_dir_path( p_directory ) || '/' || p_filename;

      IF NOT o_app.is_debugmode
      THEN
         UTL_FILE.fremove( p_directory, p_filename );
      END IF;

      o_app.log_msg( 'File ' || l_filepath || ' deleted', 3 );
      o_app.clear_app_info;
   EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         o_app.log_msg( l_filepath || ' could not be deleted, or does not exist' );
   END delete_file;

   -- uses UTL_FILE to "touch" a file
   PROCEDURE create_file(
      p_directory   VARCHAR2,
      p_filename    VARCHAR2,
      p_runmode     VARCHAR2 DEFAULT NULL
   )
   AS
      l_fh        UTL_FILE.file_type;
      l_dirpath   VARCHAR2( 100 );
      o_app       applog             := applog( p_module => 'create_file' );
   BEGIN
      l_dirpath := get_dir_path( p_directory ) || '/' || p_filename;

      IF NOT o_app.is_debugmode
      THEN
         l_fh := UTL_FILE.fopen( p_directory, p_filename, 'W' );
      END IF;

      o_app.log_msg( 'File ' || l_dirpath || ' created', 3 );
      o_app.clear_app_info;
   END create_file;

-- log a message to the log_table
-- the preferred method for using the logging framework is to instantiate a APPLOG object and use that
-- this is provided in situations where invoking an object is difficult--such as testing in SQLPLUS
-- real development pieces should use APPLOG
   PROCEDURE log_msg( p_msg log_table.msg%TYPE )
   AS
      o_app   applog := applog( p_action => SYS_CONTEXT( 'USERENV', 'ACTION' ));
   BEGIN
      o_app.log_msg( p_msg );
   END log_msg;

   -- get the number of lines in a file
   FUNCTION get_numlines(
      p_dirname    IN   VARCHAR2,                       -- this is a directory object name
      p_filename   IN   VARCHAR2,                                  -- the name of the file
      p_runmode         VARCHAR2 DEFAULT NULL
   )                                                                         -- debug mode
      RETURN NUMBER                                                     -- number of lines
   AS
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2( 2000 );
      l_cnt    NUMBER             := 0;
      o_app    applog     := applog( p_module       => 'get_numlines',
                                     p_runmode      => p_runmode );
   BEGIN
      IF o_app.is_debugmode
      THEN
         o_app.log_msg( o_app.module || ' returning 0 because of DEBUG mode' );
         o_app.clear_app_info;
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
               o_app.clear_app_info;
               RETURN l_cnt;
         END;
      END IF;
   END get_numlines;

   -- a function used to unzip a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- function returns what the name should be after the unzip process
   FUNCTION unzip_file(
      p_dirpath    VARCHAR2,
      p_filename   VARCHAR2,
      p_runmode    VARCHAR2 DEFAULT NULL
   )
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
      o_app            applog
                            := applog( p_module       => 'unzip_file',
                                       p_runmode      => p_runmode );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename, '\.[^\.]+$', NULL, 1, 1, 'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;
      o_app.log_msg( l_filepath || ' checked for compression using standard libraries',
                     3 );

      CASE l_filesuf
         WHEN 'gz'
         THEN
            host_cmd( 'gzip -df ' || l_filepath, p_runmode => o_app.runmode );
            o_app.log_msg( l_filepath || ' gunzipped', 3 );
         WHEN 'Z'
         THEN
            host_cmd( 'uncompress ' || l_filepath, p_runmode => o_app.runmode );
            o_app.log_msg( l_filepath || ' uncompressed', 3 );
         WHEN 'bz2'
         THEN
            host_cmd( 'bunzip2 ' || l_filepath, p_runmode => o_app.runmode );
            o_app.log_msg( l_filepath || ' bunzipped', 3 );
         WHEN 'zip'
         THEN
            host_cmd( 'unzip ' || l_filepath, p_runmode => o_app.runmode );
            o_app.log_msg( l_filepath || ' unzipped', 3 );
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

      IF o_app.is_debugmode
      THEN
         o_app.log_msg( 'File returned by UNZIP_FILE: ' || l_return );
      ELSE
         o_app.set_action( 'Check for extracted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize
                          );

         IF NOT l_file_exists
         THEN
            raise_application_error( get_err_cd( 'file_not_found' ),
                                     get_err_msg( 'file_not_found' )
                                   );
         END IF;
      END IF;

      o_app.clear_app_info;
      RETURN l_return;
   END unzip_file;

   -- a function used to decrypt a file regardless of which method was used to encrypt it
   -- currently contains functionality for the following encryption methods: gpg
   -- function returns what the name should be after the decryption process
   FUNCTION decrypt_file(
      p_dirpath      VARCHAR2,
      p_filename     VARCHAR2,
      p_passphrase   VARCHAR2,
      p_runmode      VARCHAR2 DEFAULT NULL
   )
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
      o_app            applog
                          := applog( p_module       => 'decrypt_file',
                                     p_runmode      => p_runmode );
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
                      p_passphrase,
                      p_runmode      => o_app.runmode
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

      IF o_app.is_debugmode
      THEN
         o_app.log_msg( 'File returned by DECRYPT_FILE: ' || l_return );
      ELSE
         o_app.set_action( 'Check for decrypted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( get_dir_name( p_dirpath ),
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

      o_app.clear_app_info;
      RETURN l_return;
   END decrypt_file;
END td_core;
/