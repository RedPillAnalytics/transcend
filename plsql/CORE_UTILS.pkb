CREATE OR REPLACE PACKAGE BODY tdinc.core_utils
AS
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
      o_app   applog := applog (p_module => 'CORE_UTILS.DDL_EXEC', p_debug => p_debug);
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
      o_app    applog                             := applog (p_module      => 'CORE_UTILS.GET_DIR_PATH');
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
      o_app       applog                          := applog (p_module      => 'CORE_UTILS.GET_DIR_NAME');
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
      o_app    applog             := applog (p_module => 'CORE_UTILS.GET_NUMLINES');
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
      o_app            applog   := applog (p_module      => 'CORE_UTILS.UNZIP_FILE',
                                           p_debug       => p_debug);
   BEGIN
      l_filebase := REGEXP_REPLACE (p_filename, '\.[^\.]+$', NULL, 1, 1, 'i');
      l_filesuf := REGEXP_SUBSTR (p_filename, '[^\.]+$');
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gz'
         THEN
            l_cmd := 'gzip -df ' || l_filepath;

            IF p_debug
            THEN
               o_app.log_msg ('Run_cmd: ' || l_cmd);
            ELSE
               util.run_cmd (l_cmd);
            END IF;
         WHEN 'Z'
         THEN
            l_cmd := 'uncompress ' || l_filepath;

            IF p_debug
            THEN
               o_app.log_msg ('Run_cmd: ' || l_cmd);
            ELSE
               util.run_cmd (l_cmd);
            END IF;
         WHEN 'bz2'
         THEN
            l_cmd := 'bunzip2 ' || l_filepath;

            IF p_debug
            THEN
               o_app.log_msg ('Run_cmd: ' || l_cmd);
            ELSE
               util.run_cmd (l_cmd);
            END IF;
         WHEN 'zip'
         THEN
            l_cmd := 'unzip ' || l_filepath;

            IF p_debug
            THEN
               o_app.log_msg ('Run_cmd: ' || l_cmd);
            ELSE
               util.run_cmd (l_cmd);
            END IF;
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
         UTL_FILE.fgetattr (core_utils.get_dir_name (p_dirpath),
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
      o_app            applog := applog (p_module      => 'CORE_UTILS.DECRYPT_FILE',
                                         p_debug       => p_debug);
   BEGIN
      l_filebase := REGEXP_REPLACE (p_filename, '\.[^\.]+$', NULL, 1, 1, 'i');
      l_filesuf := REGEXP_SUBSTR (p_filename, '[^\.]+$');
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gpg'
         THEN
            IF p_debug
            THEN
               o_app.log_msg ('File to decrypt: ' || l_filepath);
            ELSE
               util.gpg_decrypt_file (l_filepath, l_filebasepath, p_passphrase);
            END IF;
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
         UTL_FILE.fgetattr (core_utils.get_dir_name (p_dirpath),
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
      p_notification_id   notification.email_notify_id%TYPE,
      p_module            notification.module%TYPE,
      p_module_id         notification.module_id%TYPE,
      p_debug             BOOLEAN DEFAULT FALSE)
   AS
      r_notification   notification%ROWTYPE;
      l_url            VARCHAR2 (256);
      l_title          VARCHAR2 (100);
      l_rows           BOOLEAN                DEFAULT FALSE;
      l_msg            VARCHAR2 (2000);
      l_ddl            VARCHAR2 (2000);
      l_subject        VARCHAR2 (55);
      l_headers        BOOLEAN;
      l_sendmail       BOOLEAN;
      l_app            applog         := applog (p_module      => 'EXTRACTS.NOTIFY',
                                                 p_debug       => p_debug);
   BEGIN
      SELECT NVL (p_extract, EXTRACT),
             extract_number,
             NVL (p_object, OBJECT),
             NVL (p_owner, owner),
             NVL (p_filebase, filebase),
             NVL (p_filext, filext),
             NVL (p_datestamp, datestamp),
             NVL (p_dateformat, DATEFORMAT),
             NVL (p_dirname, dirname),
             NVL (p_stgdirname, stgdirname),
             NVL (p_delimiter, delimiter),
             NVL (p_quotechar, quotechar),
             NVL (p_sender, sender),
             NVL (p_recipients, recipients),
             NVL (p_baseurl, baseurl),
             NVL (p_headers, headers),
             NVL (p_sendmail, sendmail),
             NVL (p_arcdirname, arcdirname),
             created_user,
             created_dt,
             modified_user,
             modified_dt
        INTO r_extract_conf
        FROM extract_conf
       WHERE (EXTRACT = p_extract);

      l_rows := TRUE;
      l_stgdirname := NVL (p_stgdirname, c_configs.stgdirname);
      l_arcdirname := NVL (p_arcdirname, c_configs.arcdirname);
      l_dirname := NVL (p_dirname, c_configs.dirname);

      IF c_configs.headers = 'Y'
      THEN
         l_headers := TRUE;
      ELSE
         l_headers := FALSE;
      END IF;

      l_sendmail :=
         CASE
            WHEN p_sendmail IS NULL AND c_configs.sendmail = 'Y'
               THEN TRUE
            WHEN p_sendmail
               THEN TRUE
            ELSE FALSE
         END;
      l_file :=
            NVL (p_filebase, c_configs.filebase)
         || '_'
         || TO_CHAR (SYSDATE, NVL (p_datestamp, c_configs.datestamp))
         || NVL (p_filext, c_configs.filext);
      l_url := utility.format_url (NVL (p_baseurl, c_configs.baseurl) || l_file);
      l_ddl :=
           'alter session set nls_date_format=''' || NVL (p_dateformat, c_configs.DATEFORMAT)
           || '''';
      l_app.set_action ('Setting NLS_DATE_FORMAT');

      IF p_debug
      THEN
         DBMS_OUTPUT.put_line ('nls_date_format DDL: ' || l_ddl);
      ELSE
         EXECUTE IMMEDIATE l_ddl;
      END IF;

      l_app.set_action ('Extract data');

      CASE
         WHEN p_debug AND l_stgdirname IS NOT NULL
         THEN
            o_app.log_msg ('Data would be extracted to ' || l_stgdirname);
            o_app.log_msg ('Stage file would be moved to ' || l_dirname);
         WHEN NOT p_debug AND l_stgdirname IS NOT NULL
         THEN
            extract_object (p_owner          => NVL (p_owner, c_configs.owner),
                            p_object         => NVL (p_object, c_configs.OBJECT),
                            p_dirname        => l_stgdirname,
                            p_filename       => l_file,
                            p_delimiter      => NVL (p_delimiter, c_configs.delimiter),
                            p_quotechar      => NVL (p_quotechar, c_configs.quotechar),
                            p_headers        => NVL (p_headers, l_headers));
            o_app.log_msg ('Moving stage file to ' || l_dirname);
            UTL_FILE.frename (l_stgdirname, l_file, l_dirname, l_file, TRUE);
         WHEN NOT p_debug AND c_configs.stgdirname IS NULL
         THEN
            extract_object (p_owner          => NVL (p_owner, c_configs.owner),
                            p_object         => NVL (p_object, c_configs.OBJECT),
                            p_dirname        => l_dirname,
                            p_filename       => l_file,
                            p_delimiter      => NVL (p_delimiter, c_configs.delimiter),
                            p_quotechar      => NVL (p_quotechar, c_configs.quotechar),
                            p_headers        => NVL (p_headers, l_headers));
         WHEN p_debug AND c_configs.stgdirname IS NULL
         THEN
            o_app.log_msg ('Data would be extracted to ' || l_dirname);
            extract_object (p_owner          => NVL (p_owner, c_configs.owner),
                            p_object         => NVL (p_object, c_configs.OBJECT),
                            p_dirname        => l_dirname,
                            p_filename       => l_file,
                            p_delimiter      => NVL (p_delimiter, c_configs.delimiter),
                            p_quotechar      => NVL (p_quotechar, c_configs.quotechar),
                            p_headers        => NVL (p_headers, l_headers));
         ELSE
            NULL;
      END CASE;

      IF p_debug
      THEN
         DBMS_OUTPUT.put_line (   'Extract SQL: '
                               || 'select * from '
                               || NVL (p_owner, c_configs.owner)
                               || '.'
                               || NVL (p_object, c_configs.OBJECT));
      ELSE
         l_numlines := get_numlines;
         l_app.set_action ('Checking number of lines');
      END IF;

      CASE
         WHEN l_numlines > 65536
         THEN
            l_msg :=
                  '************'
               || CHR (13)
               || 'NOTE: Today''s '
               || c_configs.EXTRACT
               || ' file has '
               || l_numlines
               || ' rows'
               || CHR (13)
               || '      Excel is not capable of opening files over 65536 rows'
               || CHR (13)
               || '************';
         WHEN l_numlines = 0
         THEN
            l_msg :=
                  '************'
               || CHR (13)
               || 'NOTE: Today''s '
               || c_configs.EXTRACT
               || ' file is empty'
               || CHR (13)
               || '************';
         ELSE
            l_msg := NULL;
      END CASE;

      l_msg := l_msg || CHR (13) || CHR (13) || l_url;
      l_subject := c_configs.EXTRACT || ' Report URL' || CHR (13);

      CASE
         WHEN l_sendmail AND NOT p_debug
         THEN
            o_app.log_msg (   'Sending email to '
                           || NVL (p_recipients, c_configs.recipients)
                           || ' from '
                           || c_configs.sender);
            --                utl_mail.send (sender          => c_configs.sender,
            --                               recipients      => NVL (p_recipients, c_configs.recipients),
            --                               subject         => l_subject,
            --                               message         => l_msg,
            --                               mime_type       => 'text/html');
            utility.send_email (p_sender          => c_configs.sender,
                                p_recipients      => NVL (p_recipients, c_configs.recipients),
                                p_subject         => l_subject,
                                p_message         => l_msg);
            l_app.set_action ('Formatting email message');
         WHEN p_debug AND l_sendmail
         THEN
            DBMS_OUTPUT.put_line ('Email Subject: ' || l_subject);
            DBMS_OUTPUT.put_line ('Email MSG: ' || l_msg);
            DBMS_OUTPUT.put_line ('Email URL: ' || l_url);
         ELSE
            NULL;
      END CASE;

      CASE
         WHEN l_arcdirname IS NOT NULL AND NOT p_debug
         THEN
            UTL_FILE.fcopy (l_dirname, l_file, l_arcdirname, l_file);
         WHEN l_arcdirname IS NOT NULL AND p_debug
         THEN
            o_app.log_msg (l_file || ' would be copied to ' || l_arcdirname);
         ELSE
            NULL;
      END CASE;

      IF NOT l_rows
      THEN
         raise_application_error (-20001,
                                  'Combination of parameters yields no object to extract from.');
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END notify;
END core_utils;
/