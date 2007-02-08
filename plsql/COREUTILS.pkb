CREATE OR REPLACE PACKAGE BODY tdinc.coreutils
AS
   -- procedure executes the host_cmd function and raises an exception with the return code
   PROCEDURE host_cmd (p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ', p_debug BOOLEAN DEFAULT FALSE)
   AS
      l_retval   NUMBER;
      o_app      applog := applog (p_module => 'COREUTILS.HOST_CMD');
   BEGIN
      DBMS_JAVA.set_output (1000000);

      IF p_debug
      THEN
         o_app.log_msg ('Host command: ' || p_cmd);
      ELSE
         l_retval := host_cmd (p_cmd, p_stdin);

         IF l_retval <> 0
         THEN
            raise_application_error
                             (-20020,
                              'Java Error: method CoreUtils.hostCmd made unsuccessful system calls');
         END IF;
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END host_cmd;

-- log a message to the log_table
-- the preferred method for using the logging framework is to instantiate a APPLOG object and use that
-- this is provided in situations where invoking an object is difficult--such as testing in SQLPLUS
-- real development pieces should use APPLOG
   PROCEDURE log_msg (p_msg log_table.msg%TYPE)
   AS
      o_app   applog := applog (p_action        => SYS_CONTEXT ('USERENV', 'ACTION'),
                                p_register      => FALSE);
   BEGIN
      o_app.log_msg (p_msg);
   END log_msg;

   PROCEDURE ddl_exec (
      p_ddl         VARCHAR2,
      p_debug_msg   VARCHAR2 DEFAULT 'DDL statememt: ',
      p_debug       BOOLEAN DEFAULT FALSE)
   AS
      o_app   applog := applog (p_module => 'COREUTILS.DDL_EXEC', p_debug => p_debug);
   BEGIN
      IF p_debug
      THEN
         o_app.log_msg (p_debug_msg || p_ddl);
      ELSE
         EXECUTE IMMEDIATE p_ddl;
      END IF;
   END ddl_exec;

   -- used to get the path associated with a directory location
   FUNCTION get_dir_path (p_dirname VARCHAR2)
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
      o_app    applog                              := applog (p_module      => 'COREUTILS.GET_DIR_PATH');
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER (p_dirname);

      o_app.clear_app_info;
      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error (-20010, 'Directory object does not exist');
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name (p_dir_path VARCHAR2)
      RETURN VARCHAR2
   AS
      l_dirname   all_directories.directory_name%TYPE;
      o_app       applog                           := applog (p_module      => 'COREUTILS.GET_DIR_NAME');
   BEGIN
      SELECT directory_name
        INTO l_dirname
        FROM all_directories
       WHERE directory_path = p_dir_path;

      o_app.clear_app_info;
      RETURN l_dirname;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error (-20011, 'No directory object defined for the specified path');
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error (-20012,
                                  'More than one directory object defined for the specified path');
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END get_dir_name;

   -- get the number of lines in a file
   FUNCTION get_numlines (
      p_dirname    IN   VARCHAR2,                                 -- this is a directory object name
      p_filename   IN   VARCHAR2)                                            -- the name of the file
      RETURN NUMBER                                                               -- number of lines
   AS
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2 (2000);
      l_cnt    NUMBER             := 0;
      o_app    applog             := applog (p_module => 'COREUTILS.GET_NUMLINES');
   BEGIN
      l_fh := UTL_FILE.fopen (p_dirname, p_filename, 'R', 32767);

      LOOP
         UTL_FILE.get_line (l_fh, l_line);
         l_cnt := l_cnt + 1;
      END LOOP;

      o_app.clear_app_info;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         UTL_FILE.fclose (l_fh);
         RETURN l_cnt;
   END get_numlines;

   -- a function used to unzip a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- function returns what the name should be after the unzip process
   FUNCTION unzip_file (p_dirpath VARCHAR2, p_filename VARCHAR2, p_debug BOOLEAN DEFAULT FALSE)
      RETURN VARCHAR2
   AS
      l_compressed     BOOLEAN        := TRUE;
      l_filebase       VARCHAR2 (50);
      l_filesuf        VARCHAR2 (20);
      l_filepath       VARCHAR2 (200) := p_dirpath || '/' || p_filename;
      l_filebasepath   VARCHAR2 (200);
      l_cmd            VARCHAR2 (200);
      l_return         VARCHAR2 (200);
      l_file_exists    BOOLEAN;
      l_file_size      NUMBER;
      l_blocksize      NUMBER;
      o_app            applog    := applog (p_module      => 'COREUTILS.UNZIP_FILE',
                                            p_debug       => p_debug);
   BEGIN
      l_filebase := REGEXP_REPLACE (p_filename, '\.[^\.]+$', NULL, 1, 1, 'i');
      l_filesuf := REGEXP_SUBSTR (p_filename, '[^\.]+$');
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gz'
         THEN
            host_cmd ('gzip -df ' || l_filepath, p_debug => p_debug);
         WHEN 'Z'
         THEN
            host_cmd ('uncompress ' || l_filepath, p_debug => p_debug);
         WHEN 'bz2'
         THEN
            host_cmd ('bunzip2 ' || l_filepath, p_debug => p_debug);
         WHEN 'zip'
         THEN
            host_cmd ('unzip ' || l_filepath, p_debug => p_debug);
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

      IF p_debug
      THEN
         o_app.log_msg ('File returned by UNZIP_FILE: ' || l_return);
      ELSE
         o_app.set_action ('Check for extracted file');
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr (coreutils.get_dir_name (p_dirpath),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize);

         IF NOT l_file_exists
         THEN
            raise_application_error (-20020, 'Filename to return does not exist');
         END IF;
      END IF;

      o_app.clear_app_info;
      RETURN l_return;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END unzip_file;

   -- a function used to decrypt a file regardless of which method was used to encrypt it
   -- currently contains functionality for the following encryption methods: gpg
   -- function returns what the name should be after the decryption process
   FUNCTION decrypt_file (
      p_dirpath      VARCHAR2,
      p_filename     VARCHAR2,
      p_passphrase   VARCHAR2,
      p_debug        BOOLEAN DEFAULT FALSE)
      RETURN VARCHAR2
   AS
      l_encrypted      BOOLEAN        := TRUE;
      l_filebase       VARCHAR2 (50);
      l_filesuf        VARCHAR2 (20);
      l_filepath       VARCHAR2 (200) := p_dirpath || '/' || p_filename;
      l_filebasepath   VARCHAR2 (200);
      l_cmd            VARCHAR2 (200);
      l_return         VARCHAR2 (200);
      l_file_exists    BOOLEAN;
      l_file_size      NUMBER;
      l_blocksize      NUMBER;
      o_app            applog  := applog (p_module      => 'COREUTILS.DECRYPT_FILE',
                                          p_debug       => p_debug);
   BEGIN
      l_filebase := REGEXP_REPLACE (p_filename, '\.[^\.]+$', NULL, 1, 1, 'i');
      l_filesuf := REGEXP_SUBSTR (p_filename, '[^\.]+$');
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gpg'
         THEN
            host_cmd (   'gpg --no-tty --passphrase-fd 0 --batch --decrypt --output '
                      || l_filepath
                      || ' '
                      || l_filebasepath,
                      p_passphrase,
                      p_debug      => p_debug);
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

      IF p_debug
      THEN
         o_app.log_msg ('File returned by DECRYPT_FILE: ' || l_return);
      ELSE
         o_app.set_action ('Check for decrypted file');
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr (coreutils.get_dir_name (p_dirpath),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize);

         IF NOT l_file_exists
         THEN
            raise_application_error (-20020, 'Filename to return does not exist');
         END IF;
      END IF;

      o_app.clear_app_info;
      RETURN l_return;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END decrypt_file;

   -- extract data to a text file, and then peform other functions as defined in the configuration table
   PROCEDURE notify (
      p_notification_type   notification.notification_type%TYPE,
      p_component           notification.component%TYPE,
      p_component_id        notification.component_id%TYPE,
      p_sender              notification.sender%TYPE DEFAULT NULL,
      p_recipients          notification.recipients%TYPE DEFAULT NULL,
      p_baseurl             notification.baseurl%TYPE DEFAULT NULL,
      p_debug               BOOLEAN DEFAULT FALSE)
   AS
      r_notification   notification%ROWTYPE;
      o_app            applog        := applog (p_module      => 'COREUTILS.NOTIFY',
                                                p_debug       => p_debug);
   BEGIN
      SELECT p_notification_type,
             p_component,
             p_component_id,
             NVL (p_sender, sender),
             NVL (p_recipients, recipients),
             NVL (p_baseurl, baseurl),
             created_user,
             created_dt,
             modified_user,
             modified_dt
        INTO r_notification
        FROM notification
       WHERE notification_type = p_notification_type
         AND component = p_component
         AND component_id = p_component_id;

      CASE notification_type
         WHEN 'extract_alert'
         THEN
            NULL;
         ELSE
            NULL;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_app.log_msg ('Notification not configured');
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END notify;
END coreutils;
/