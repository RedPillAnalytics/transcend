CREATE OR REPLACE PACKAGE BODY tdinc.coreutils
AS
   -- procedure executes the host_cmd function and raises an exception with the return code
   PROCEDURE host_cmd (p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ', p_runmode VARCHAR2
            DEFAULT NULL)
   AS
      l_retval   NUMBER;
      o_app      applog := applog (p_module => 'coreutils.host_cmd');
   BEGIN
      DBMS_JAVA.set_output (1000000);
      o_app.log_msg ('Host command: ' || p_cmd, 3);

      IF NOT o_app.is_debugmode
      THEN
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

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE copy_file (p_srcfile VARCHAR2, p_dstfile VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL)
   AS
      l_retval   NUMBER;
      o_app      applog := applog (p_module => 'coreutils.copy_file', p_runmode => p_runmode);
   BEGIN
      DBMS_JAVA.set_output (1000000);
      o_app.log_msg ('File ' || p_srcfile || ' would be copied to ' || p_dstfile, 3);

      IF NOT o_app.is_debugmode
      THEN
         l_retval := copy_file (p_srcfile, p_dstfile);

         IF l_retval <> 0
         THEN
            raise_application_error (-20020,
                                        'Java Error: method CoreUtils.copyFile was unable to copy '
                                     || p_srcfile
                                     || ' to '
                                     || p_dstfile);
         END IF;
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END copy_file;

   -- uses UTL_FILE to remove an OS level file
   PROCEDURE delete_file (p_directory VARCHAR2, p_filename VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL)
   AS
      l_retval     NUMBER;
      l_filepath   VARCHAR2 (100);
      o_app        applog   := applog (p_module       => 'coreutils.delete_file',
                                       p_runmode      => p_runmode);
   BEGIN
      l_filepath := coreutils.get_dir_path (p_directory) || '/' || p_filename;
      o_app.log_msg ('File to delete: ' || l_filepath, 3);

      IF NOT o_app.is_debugmode
      THEN
         UTL_FILE.fremove (p_directory, p_filename);
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         o_app.log_msg (l_filepath || ' could not be deleted. It probably does not exist');
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END delete_file;

   -- uses UTL_FILE to "touch" a file
   PROCEDURE create_file (p_directory VARCHAR2, p_filename VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL)
   AS
      l_fh        UTL_FILE.file_type;
      l_dirpath   VARCHAR2 (100);
      o_app       applog             := applog (p_module => 'coreutils.create_file');
   BEGIN
      l_dirpath := get_dir_path (p_directory) || '/' || p_filename;

      IF o_app.is_debugmode
      THEN
         o_app.log_msg ('File to create: ' || l_dirpath);
      ELSE
         l_fh := UTL_FILE.fopen (p_directory, p_filename, 'W');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END create_file;

-- log a message to the log_table
-- the preferred method for using the logging framework is to instantiate a APPLOG object and use that
-- this is provided in situations where invoking an object is difficult--such as testing in SQLPLUS
-- real development pieces should use APPLOG
   PROCEDURE log_msg (p_msg log_table.msg%TYPE)
   AS
      o_app   applog := applog (p_action => SYS_CONTEXT ('USERENV', 'ACTION'));
   BEGIN
      o_app.log_msg (p_msg);
   END log_msg;

   PROCEDURE exec_auto (
      p_ddl           VARCHAR2,
      p_runmode       VARCHAR2 DEFAULT NULL,
      p_runmode_msg   VARCHAR2 DEFAULT 'DDL: ')
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      o_app   applog := applog (p_module => 'coreutils.exec_auto', p_runmode => p_runmode);
   BEGIN
      o_app.log_msg (p_runmode_msg || p_ddl, 3);

      IF NOT o_app.is_debugmode
      THEN
         EXECUTE IMMEDIATE p_ddl;
      END IF;

      COMMIT;
   END exec_auto;

   PROCEDURE exec_sql (
      p_dml           VARCHAR2,
      p_runmode       VARCHAR2 DEFAULT NULL,
      p_runmode_msg   VARCHAR2 DEFAULT 'DML: ')
   AS
      o_app   applog := applog (p_module => 'coreutils.exec_sql', p_runmode => p_runmode);
   BEGIN
      o_app.log_msg (p_runmode_msg || p_dml, 3);

      IF NOT o_app.is_debugmode
      THEN
         EXECUTE IMMEDIATE p_dml;
      END IF;
   END exec_sql;

   -- used to get the path associated with a directory location
   FUNCTION get_dir_path (p_dirname VARCHAR2)
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER (p_dirname);

      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error (-20010, 'Directory object does not exist');
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name (p_dir_path VARCHAR2)
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
         raise_application_error (-20011, 'No directory object defined for the specified path');
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error (-20012,
                                  'More than one directory object defined for the specified path');
   END get_dir_name;

   -- GET method for pulling an error code out of the ERR_CD table
   FUNCTION get_err_cd (p_name VARCHAR2)
      RETURN NUMBER
   AS
      l_code   err_cd.code%TYPE;
   BEGIN
      SELECT code
        INTO l_code
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_code;
   END get_err_cd;

   -- GET method for pulling error text out of the ERR_CD table
   FUNCTION get_err_msg (p_name VARCHAR2)
      RETURN VARCHAR2
   AS
      l_msg   err_cd.MESSAGE%TYPE;
   BEGIN
      SELECT MESSAGE
        INTO l_msg
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_msg;
   END get_err_msg;

   -- returns a boolean
   -- does a check to see if a table exists
   -- raises an error if it doesn't
   FUNCTION table_exists (p_owner VARCHAR2, p_table VARCHAR2)
      RETURN BOOLEAN
   AS
      l_table   dba_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM dba_tables
       WHERE owner = UPPER (p_owner) AND table_name = UPPER (p_table);

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END table_exists;

   -- returns a boolean
   -- does a check to see if table is partitioned
   -- raises an error if it doesn't
   FUNCTION is_part_table (p_owner VARCHAR2, p_table VARCHAR2)
      RETURN BOOLEAN
   AS
      l_partitioned   dba_tables.partitioned%TYPE;
   BEGIN
      SELECT partitioned
        INTO l_partitioned
        FROM dba_tables
       WHERE owner = UPPER (p_owner) AND table_name = UPPER (p_table);

      CASE
         WHEN is_true (l_partitioned)
         THEN
            RETURN TRUE;
         WHEN NOT is_true (l_partitioned)
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
   -- raises an error if it doesn't
   FUNCTION object_exists (p_owner VARCHAR2, p_object VARCHAR2)
      RETURN BOOLEAN
   AS
      l_object   dba_objects.object_name%TYPE;
      o_app      applog                         := applog (p_module => 'coreutils.object_exists');
   BEGIN
      SELECT DISTINCT object_name
                 INTO l_object
                 FROM dba_objects
                WHERE owner = UPPER (p_owner) AND object_name = UPPER (p_object);

      o_app.clear_app_info;
      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_app.clear_app_info;
         RETURN FALSE;
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END object_exists;

   -- returns a boolean
   -- accepts a varchar2 and determines if regexp matches 'yes' or 'no'
   -- raises an error if it doesn't
   FUNCTION is_true (p_parm VARCHAR2)
      RETURN BOOLEAN
   AS
   BEGIN
      -- use the load_tab or merge_tab procedure depending on P_MERGE
      CASE
         WHEN REGEXP_LIKE ('yes', p_parm, 'i')
         THEN
            RETURN TRUE;
         WHEN REGEXP_LIKE ('no', p_parm, 'i')
         THEN
            RETURN FALSE;
         ELSE
            raise_application_error (get_err_cd ('unrecognized_parm'),
                                     get_err_msg ('unrecognized_parm') || ' : ' || p_parm);
      END CASE;
   END is_true;

   -- much like IS_TRUE above, but BOOLEANS, though useful in PL/SQL, are not supported in SQL
   -- this can be used in SQL cursors
   -- returns a varchar2
   -- accepts a varchar2 and determines if regexp matches 'yes' or 'no'
   -- raises an error if it doesn't
   FUNCTION get_yn_ind (p_parm VARCHAR2)
      RETURN VARCHAR2
   AS
   BEGIN
      -- use the load_tab or merge_tab procedure depending on P_MERGE
      CASE
         WHEN REGEXP_LIKE ('yes', p_parm, 'i')
         THEN
            RETURN 'yes';
         WHEN REGEXP_LIKE ('no', p_parm, 'i')
         THEN
            RETURN 'no';
         ELSE
            raise_application_error (get_err_cd ('unrecognized_parm'),
                                     get_err_msg ('unrecognized_parm') || ' : ' || p_parm);
      END CASE;
   END get_yn_ind;

   -- get the number of lines in a file
   FUNCTION get_numlines (
      p_dirname    IN   VARCHAR2,                                 -- this is a directory object name
      p_filename   IN   VARCHAR2,                                            -- the name of the file
      p_runmode         VARCHAR2 DEFAULT NULL)                                         -- debug mode
      RETURN NUMBER                                                               -- number of lines
   AS
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2 (2000);
      l_cnt    NUMBER             := 0;
      o_app    applog      := applog (p_module       => 'coreutils.get_numlines',
                                      p_runmode      => p_runmode);
   BEGIN
      IF o_app.is_debugmode
      THEN
         o_app.log_msg (o_app.module || ' returning 0 because of DEBUG mode');
         RETURN 0;
      ELSE
         BEGIN
            l_fh := UTL_FILE.fopen (p_dirname, p_filename, 'R', 32767);

            LOOP
               UTL_FILE.get_line (l_fh, l_line);
               l_cnt := l_cnt + 1;
            END LOOP;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               UTL_FILE.fclose (l_fh);
               o_app.clear_app_info;
               RETURN l_cnt;
         END;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END get_numlines;

   -- a function used to unzip a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- function returns what the name should be after the unzip process
   FUNCTION unzip_file (p_dirpath VARCHAR2, p_filename VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL)
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
      o_app            applog
                             := applog (p_module       => 'coreutils.unzip_file',
                                        p_runmode      => p_runmode);
   BEGIN
      l_filebase := REGEXP_REPLACE (p_filename, '\.[^\.]+$', NULL, 1, 1, 'i');
      l_filesuf := REGEXP_SUBSTR (p_filename, '[^\.]+$');
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gz'
         THEN
            host_cmd ('gzip -df ' || l_filepath, p_runmode => p_runmode);
         WHEN 'Z'
         THEN
            host_cmd ('uncompress ' || l_filepath, p_runmode => p_runmode);
         WHEN 'bz2'
         THEN
            host_cmd ('bunzip2 ' || l_filepath, p_runmode => p_runmode);
         WHEN 'zip'
         THEN
            host_cmd ('unzip ' || l_filepath, p_runmode => p_runmode);
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
      p_runmode      VARCHAR2 DEFAULT NULL)
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
      o_app            applog
                           := applog (p_module       => 'coreutils.decrypt_file',
                                      p_runmode      => p_runmode);
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
                      p_runmode      => p_runmode);
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
END coreutils;
/