CREATE OR REPLACE PACKAGE BODY efw.utility
AS
-- log a message to the log_table
-- the preferred method for using the logging framework is to instantiate a APPLOG object and use that
-- this is provided in situations where invoking an object is difficult--such as testing in SQLPLUS
-- real development pieces should use APPLOG
   PROCEDURE log_msg(
      p_msg   log_table.msg%TYPE )
   AS
      o_app   applog := applog( p_action=>sys_context('USERENV','ACTION'),
				p_register =>      FALSE );
   BEGIN
      o_app.log_msg( p_msg );
   END log_msg;
   
   -- used to get the path associated with a directory location
   FUNCTION get_dir_path(
      p_dirname   VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
      o_app    app_info                           := app_info( p_module =>      'UTILITY.GET_DIR_PATH' );
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER( p_dirname );

      o_app.clear_app_info;
      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error( -20010, 'Directory object does not exist' );
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name(
      p_dir_path   VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_dirname   all_directories.directory_name%TYPE;
      o_app       app_info                        := app_info( p_module =>      'UTILITY.GET_DIR_NAME' );
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
         raise_application_error( -20011, 'No directory object defined for the specified path' );
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error( -20012,
                                  'More than one directory object defined for the specified path' );
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END get_dir_name;

   -- get the number of lines in a file
   FUNCTION get_numlines(
      p_dirname    IN   VARCHAR2,                                 -- this is a directory object name
      p_filename   IN   VARCHAR2 )                                           -- the name of the file
      RETURN NUMBER                                                               -- number of lines
   AS
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2( 2000 );
      l_cnt    NUMBER             := 0;
      o_app    app_info           := app_info( p_module =>      'UTILITY.GET_NUMLINES' );
   BEGIN
      l_fh := UTL_FILE.fopen( p_dirname,
                              p_filename,
                              'R',
                              32767 );

      LOOP
         UTL_FILE.get_line( l_fh, l_line );
         l_cnt := l_cnt + 1;
      END LOOP;

      o_app.clear_app_info;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         UTL_FILE.fclose( l_fh );
         RETURN l_cnt;
   END get_numlines;

   -- wrote this procedure because UTL_MAIL had bugs in Oracle 10.1
   PROCEDURE send_email(
      p_recipients      IN   VARCHAR2,
      p_sender          IN   VARCHAR2 DEFAULT 'no-reply@transcendentdata.com',
      p_subject         IN   VARCHAR2,
      p_message         IN   VARCHAR2,
      p_smtp_hostname   IN   VARCHAR2 DEFAULT 'localhost',
      p_smtp_portnum    IN   VARCHAR2 DEFAULT '25',
      p_pre_html             BOOLEAN DEFAULT FALSE )
   IS
      l_boundary     VARCHAR2( 255 )     DEFAULT 'a1b2c3d4e3f2g1';
      l_connection   UTL_SMTP.connection;
      l_body_html    CLOB                := EMPTY_CLOB;        --This LOB will be the email message
      l_offset       NUMBER;
      l_ammount      NUMBER;
      l_temp         VARCHAR2( 32767 )   DEFAULT NULL;
      l_text         VARCHAR2( 2000 )    := p_message;
      l_html         VARCHAR2( 2000 )    := p_message;
      l_to           VARCHAR2( 100 );
      l_recipients   VARCHAR2( 2000 )    := p_recipients;
      o_app          app_info            := app_info( p_module =>      'UTILITY.SEND_EMAIL' );
   BEGIN
      IF p_pre_html
      THEN
         l_html := '<pre>' || l_html || '</pre>';
      END IF;

      l_connection := UTL_SMTP.open_connection( p_smtp_hostname, p_smtp_portnum );
      UTL_SMTP.helo( l_connection, p_smtp_hostname );
      UTL_SMTP.mail( l_connection, p_sender );

      WHILE REGEXP_LIKE( l_recipients, ',' )
      LOOP
         l_to := REGEXP_SUBSTR( l_recipients, ',[^,]+@[^,]+$' );
         l_recipients := REGEXP_REPLACE( l_recipients, l_to );
         l_to := REGEXP_REPLACE( l_to, ',' );
         UTL_SMTP.rcpt( l_connection, l_to );
      END LOOP;

      UTL_SMTP.rcpt( l_connection, l_recipients );
      l_temp := l_temp || 'MIME-Version: 1.0' || CHR( 13 ) || CHR( 10 );
      l_temp := l_temp || 'To: ' || p_recipients || CHR( 13 ) || CHR( 10 );
      l_temp := l_temp || 'From: ' || p_sender || CHR( 13 ) || CHR( 10 );
      l_temp := l_temp || 'Subject: ' || p_subject || CHR( 13 ) || CHR( 10 );
      l_temp := l_temp || 'Reply-To: ' || p_sender || CHR( 13 ) || CHR( 10 );
      l_temp :=
            l_temp
         || 'Content-Type: multipart/alternative; boundary='
         || CHR( 34 )
         || l_boundary
         || CHR( 34 )
         || CHR( 13 )
         || CHR( 10 );
----------------------------------------------------
-- Write the headers
      DBMS_LOB.createtemporary( l_body_html,
                                FALSE,
                                10 );
      DBMS_LOB.WRITE( l_body_html,
                      LENGTH( l_temp ),
                      1,
                      l_temp );
----------------------------------------------------
-- Write the text boundary
      l_offset := DBMS_LOB.getlength( l_body_html ) + 1;
      l_temp := '--' || l_boundary || CHR( 13 ) || CHR( 10 );
      l_temp :=
            l_temp
         || 'content-type: text/plain; charset=us-ascii'
         || CHR( 13 )
         || CHR( 10 )
         || CHR( 13 )
         || CHR( 10 );
      DBMS_LOB.WRITE( l_body_html,
                      LENGTH( l_temp ),
                      l_offset,
                      l_temp );
----------------------------------------------------
-- Write the plain text portion of the email
      l_offset := DBMS_LOB.getlength( l_body_html ) + 1;
      DBMS_LOB.WRITE( l_body_html,
                      LENGTH( l_text ),
                      l_offset,
                      l_text );
----------------------------------------------------
-- Write the HTML boundary
      l_temp :=
         CHR( 13 ) || CHR( 10 ) || CHR( 13 ) || CHR( 10 ) || '--' || l_boundary || CHR( 13 )
         || CHR( 10 );
      l_temp :=
               l_temp || 'content-type: text/html;' || CHR( 13 ) || CHR( 10 ) || CHR( 13 )
               || CHR( 10 );
      l_offset := DBMS_LOB.getlength( l_body_html ) + 1;
      DBMS_LOB.WRITE( l_body_html,
                      LENGTH( l_temp ),
                      l_offset,
                      l_temp );
----------------------------------------------------
-- Write the HTML portion of the message
      l_offset := DBMS_LOB.getlength( l_body_html ) + 1;
      DBMS_LOB.WRITE( l_body_html,
                      LENGTH( l_html ),
                      l_offset,
                      l_html );
----------------------------------------------------
-- Write the final html boundary
      l_temp := CHR( 13 ) || CHR( 10 ) || '--' || l_boundary || '--' || CHR( 13 );
      l_offset := DBMS_LOB.getlength( l_body_html ) + 1;
      DBMS_LOB.WRITE( l_body_html,
                      LENGTH( l_temp ),
                      l_offset,
                      l_temp );
----------------------------------------------------
-- Send the email in 1900 byte chunks to UTL_SMTP
      l_offset := 1;
      l_ammount := 1900;
      UTL_SMTP.open_data( l_connection );

      WHILE l_offset < DBMS_LOB.getlength( l_body_html )
      LOOP
         UTL_SMTP.write_data( l_connection, DBMS_LOB.SUBSTR( l_body_html,
                                                             l_ammount,
                                                             l_offset ));
         l_offset := l_offset + l_ammount;
         l_ammount := LEAST( 1900, DBMS_LOB.getlength( l_body_html ) - l_ammount );
      END LOOP;

      UTL_SMTP.close_data( l_connection );
      UTL_SMTP.quit( l_connection );
      DBMS_LOB.freetemporary( l_body_html );
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END send_email;

   -- takes a standard URL and return that URL formatted for HTML
   -- used in SEND_EMAIL but can be used for anything
   FUNCTION format_url(
      p_url   VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_url   VARCHAR2( 256 );
      o_app   app_info        := app_info( p_module =>      'UTILITY.FORMAT_URL' );
   BEGIN
      l_url := '<p><a href="' || p_url || '">' || p_url || '</a></p>';
      o_app.clear_app_info;
      RETURN l_url;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END format_url;

   -- a function used to unzip a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- function returns what the name should be after the unzip process
   FUNCTION unzip_file(
      p_dirpath    VARCHAR2,
      p_filename   VARCHAR2,
      p_debug      BOOLEAN DEFAULT FALSE )
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
      o_app            app_info := app_info( p_module =>      'UTILITY.UNZIP_FILE',
                                             p_debug =>       p_debug );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename,
                                    '\.[^\.]+$',
                                    NULL,
                                    1,
                                    1,
                                    'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gz'
         THEN
            l_cmd := 'gzip -df ' || l_filepath;

            IF p_debug
            THEN
               job.log_msg( 'Run_cmd: ' || l_cmd );
            ELSE
               util.run_cmd( l_cmd );
            END IF;
         WHEN 'Z'
         THEN
            l_cmd := 'uncompress ' || l_filepath;

            IF p_debug
            THEN
               job.log_msg( 'Run_cmd: ' || l_cmd );
            ELSE
               util.run_cmd( l_cmd );
            END IF;
         WHEN 'bz2'
         THEN
            l_cmd := 'bunzip2 ' || l_filepath;

            IF p_debug
            THEN
               job.log_msg( 'Run_cmd: ' || l_cmd );
            ELSE
               util.run_cmd( l_cmd );
            END IF;
         WHEN 'zip'
         THEN
            l_cmd := 'unzip ' || l_filepath;

            IF p_debug
            THEN
               job.log_msg( 'Run_cmd: ' || l_cmd );
            ELSE
               util.run_cmd( l_cmd );
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
         job.log_msg( 'File returned by UNZIP_FILE: ' || l_return );
      ELSE
         o_app.set_action( 'Check for extracted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( utility.get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize );

         IF NOT l_file_exists
         THEN
            raise_application_error( -20020, 'Filename to return does not exist' );
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
   FUNCTION decrypt_file(
      p_dirpath      VARCHAR2,
      p_filename     VARCHAR2,
      p_passphrase   VARCHAR2,
      p_debug        BOOLEAN DEFAULT FALSE )
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
      o_app            app_info
                              := app_info( p_module =>      'UTILITY.DECRYPT_FILE',
                                           p_debug =>       p_debug );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename,
                                    '\.[^\.]+$',
                                    NULL,
                                    1,
                                    1,
                                    'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gpg'
         THEN
            IF p_debug
            THEN
               job.log_msg( 'File to decrypt: ' || l_filepath );
            ELSE
               util.gpg_decrypt_file( l_filepath,
                                      l_filebasepath,
                                      p_passphrase );
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
         job.log_msg( 'File returned by DECRYPT_FILE: ' || l_return );
      ELSE
         o_app.set_action( 'Check for decrypted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( utility.get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize );

         IF NOT l_file_exists
         THEN
            raise_application_error( -20020, 'Filename to return does not exist' );
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
END utility;
/