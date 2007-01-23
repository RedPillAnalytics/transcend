CREATE OR REPLACE PACKAGE BODY efw.file_extract
AS
   g_numlines   NUMBER;

-- modified FROM tom kyte's "dump_csv":
-- 1. rewrote AS a PROCEDURE
-- 2. allow a quote CHARACTER
-- 3. allow FOR a FILE TO be appended TO
   PROCEDURE extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE )
   IS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2( 2000 );
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2( 5 )      DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
      l_mode          VARCHAR2( 1 )      DEFAULT 'w';
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_var, -1007 );
      l_app           app_info           := app_info( p_module =>      'EXTRACTS.EXTRACT_QUERY' );
   BEGIN
      IF p_append
      THEN
         l_mode := 'a';
      END IF;

      l_output := UTL_FILE.fopen( p_dirname,
                                  p_filename,
                                  l_mode,
                                  32767 );
      DBMS_SQL.parse( l_thecursor,
                      p_query,
                      DBMS_SQL.native );
      l_app.set_action( 'Open Cursor to define columns' );

      FOR i IN 1 .. 255
      LOOP
         BEGIN
            DBMS_SQL.define_column( l_thecursor,
                                    i,
                                    l_columnvalue,
                                    2000 );
            l_colcnt := i;
         EXCEPTION
            WHEN e_no_var
            THEN
               EXIT;
         END;
      END LOOP;

      DBMS_SQL.define_column( l_thecursor,
                              1,
                              l_columnvalue,
                              2000 );
      l_status := DBMS_SQL.EXECUTE( l_thecursor );
      job.log_msg( 'Extracting data to ' || p_filename || ' in directory ' || p_dirname );
      l_app.set_action( 'Open Cursor to pull back records' );

      LOOP
         EXIT WHEN( DBMS_SQL.fetch_rows( l_thecursor ) <= 0 );
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE( l_thecursor,
                                   i,
                                   l_columnvalue );
            UTL_FILE.put( l_output, l_delimiter || p_quotechar || l_columnvalue || p_quotechar );
            l_delimiter := p_delimiter;
         END LOOP;

         UTL_FILE.new_line( l_output );
         l_cnt := l_cnt + 1;
      END LOOP;

      g_numlines := l_cnt;
      job.log_msg( g_numlines || ' rows extracted to ' || p_filename );
      l_app.set_action( 'Close DBMS_SQL cursor and filehandles.' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );
      l_app.clear_app_info;
   END extract_query;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   PROCEDURE extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE,
      p_headers     BOOLEAN DEFAULT FALSE )
   IS
      l_app   app_info
         := app_info( p_module =>      'EXTRACTS.EXTRACT_OBJECT',
                      p_action =>      'Extract headers using EXTRACT_QUERY' );
   BEGIN
      IF p_headers
      THEN
         extract_query(    'select regexp_replace(stragg(column_name),'','','''
                        || p_delimiter
                        || ''') from '
                        || '(select column_name from all_tab_cols '
                        || 'where table_name='''
                        || UPPER( p_object )
                        || ''' and owner='''
                        || UPPER( p_owner )
                        || ''' order by column_id)',
                        p_dirname,
                        p_filename,
                        p_delimiter,
                        NULL,
                        p_append );
      END IF;

      l_app.set_action( 'Extract data using EXTRACT_QUERY' );
      extract_query( 'select * from ' || p_owner || '.' || p_object,
                     p_dirname,
                     p_filename,
                     p_delimiter,
                     p_quotechar,
                     TRUE );
      l_app.clear_app_info;
   END extract_object;

   -- returns the number of rows (or lines) produced from the last call to extract_query.
   FUNCTION get_numlines
      RETURN NUMBER
   AS
      l_app   app_info := app_info( p_module =>      'EXTRACTS.GET_NUMLINES' );
   BEGIN
      RETURN g_numlines;
   END get_numlines;

   -- extract all the tables or views matching a particular regular expression to a file matching the table or view name
   -- coded for case-sensitivity, so files can have mixed-case names
   -- the filename will be exactly what the object name is... plus the value FOR p_filext
   -- i am using the clCASE-insensitive regular expression match parameter for matching
   PROCEDURE extract_regexp(
      p_owner       VARCHAR2,
      p_regexp      VARCHAR2,
      p_filext      VARCHAR2 DEFAULT '.csv',
      p_dirname     VARCHAR2 DEFAULT 'MAIL_DIR',
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '"' )
   AS
      l_rows   BOOLEAN          := FALSE;
      l_sql    VARCHAR2( 2000 );
      l_app    app_info         := app_info( p_module =>      'EXTRACTS.EXTRACT_REGEXP' );
   BEGIN
      job.log_msg( 'Extracting data for the ' || p_owner
                   || ' schema matching the regular expression' );

      FOR c_objects IN ( SELECT owner,
                                object_name
                          FROM all_objects
                         WHERE REGEXP_LIKE( object_name,
                                            p_regexp,
                                            'i' )
                           AND owner = p_owner
                           AND object_type IN( 'TABLE', 'VIEW', 'SYNONYM' ))
      LOOP
         l_rows := TRUE;
         l_sql := 'select * from ' || c_objects.owner || '."' || c_objects.object_name || '"';
         extract_query( l_sql,
                        p_dirname,
                        c_objects.object_name || p_filext,
                        p_delimiter,
                        p_quotechar );
      END LOOP;

      IF NOT l_rows
      THEN
         raise_application_error( -20001,
                                  'The regular expression returns no objects for extraction.' );
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END extract_regexp;

   -- extract data to a text file, and then peform other functions as defined in the configuration table
   PROCEDURE process_extract(
      p_extract      extract_conf.EXTRACT%TYPE,
      -- The name of the report to generate. This is the PK for the table.
      p_object       extract_conf.OBJECT%TYPE DEFAULT NULL,
      -- The name of the object to extract: a table or view typically.
      p_owner        extract_conf.owner%TYPE DEFAULT NULL,               -- The owner of the object.
      p_filebase     extract_conf.filebase%TYPE DEFAULT NULL,
      -- Basename of the extract file... minus the datastamp and file extension.
      p_filext       extract_conf.filext%TYPE DEFAULT NULL,
      -- Extension to place at the end of a file
      p_datestamp    extract_conf.datestamp%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for the file datestamp
      p_dateformat   extract_conf.DATEFORMAT%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for any date columns in the file
      p_dirname      extract_conf.dirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to stage the file in initially
      p_stgdirname   extract_conf.stgdirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to extract to.
      p_delimiter    extract_conf.delimiter%TYPE DEFAULT NULL,
      -- Column delimiter in the extract file.
      p_quotechar    extract_conf.quotechar%TYPE DEFAULT NULL,
      -- Character (if any) to use to quote columns.
      p_recipients   extract_conf.recipients%TYPE DEFAULT NULL,
      -- comma separated list of recipients
      p_baseurl      extract_conf.baseurl%TYPE DEFAULT NULL,
      -- URL (minus filename) of the link to the file
      p_headers      BOOLEAN DEFAULT NULL,                 -- whether to include headers in the file
      p_sendmail     BOOLEAN DEFAULT NULL,           -- whether to send an email announcing the link
      p_arcdirname   extract_conf.arcdirname%TYPE DEFAULT NULL,
      p_debug        BOOLEAN DEFAULT FALSE )                                           -- debug mode
   AS
      l_object       all_objects.object_name%TYPE;
      l_file         VARCHAR2( 50 );
      l_numlines     NUMBER;
      l_url          VARCHAR2( 256 );
      l_title        VARCHAR2( 100 );
      l_rows         BOOLEAN                        DEFAULT FALSE;
      l_msg          VARCHAR2( 2000 );
      l_ddl          VARCHAR2( 2000 );
      l_subject      VARCHAR2( 55 );
      l_headers      BOOLEAN;
      l_sendmail     BOOLEAN;
      l_dirname      extract_conf.dirname%TYPE;
      l_arcdirname   extract_conf.arcdirname%TYPE;
      l_stgdirname   extract_conf.stgdirname%TYPE;
      l_app          app_info
                          := app_info( p_module =>      'EXTRACTS.PROCESS_EXTRACT',
                                       p_debug =>       p_debug );
   BEGIN
      FOR c_configs IN ( SELECT *
                          FROM extract_conf
                         WHERE EXTRACT = p_extract )
      LOOP
         l_rows := TRUE;
         l_stgdirname := NVL( p_stgdirname, c_configs.stgdirname );
         l_arcdirname := NVL( p_arcdirname, c_configs.arcdirname );
         l_dirname := NVL( p_dirname, c_configs.dirname );

         IF c_configs.headers = 'Y'
         THEN
            l_headers := TRUE;
         ELSE
            l_headers := FALSE;
         END IF;

         l_sendmail :=
            CASE
               WHEN p_sendmail IS NULL
               AND c_configs.sendmail = 'Y'
                  THEN TRUE
               WHEN p_sendmail
                  THEN TRUE
               ELSE FALSE
            END;
         l_file :=
               NVL( p_filebase, c_configs.filebase )
            || '_'
            || TO_CHAR( SYSDATE, NVL( p_datestamp, c_configs.datestamp ))
            || NVL( p_filext, c_configs.filext );
         l_url := utility.format_url( NVL( p_baseurl, c_configs.baseurl ) || l_file );
         l_ddl :=
               'alter session set nls_date_format='''
            || NVL( p_dateformat, c_configs.DATEFORMAT )
            || '''';
         l_app.set_action( 'Setting NLS_DATE_FORMAT' );

         IF p_debug
         THEN
            DBMS_OUTPUT.put_line( 'nls_date_format DDL: ' || l_ddl );
         ELSE
            EXECUTE IMMEDIATE l_ddl;
         END IF;

         l_app.set_action( 'Extract data' );

         CASE
            WHEN p_debug
            AND l_stgdirname IS NOT NULL
            THEN
               job.log_msg( 'Data would be extracted to ' || l_stgdirname );
               job.log_msg( 'Stage file would be moved to ' || l_dirname );
            WHEN NOT p_debug
            AND l_stgdirname IS NOT NULL
            THEN
               extract_object( p_owner =>          NVL( p_owner, c_configs.owner ),
                               p_object =>         NVL( p_object, c_configs.OBJECT ),
                               p_dirname =>        l_stgdirname,
                               p_filename =>       l_file,
                               p_delimiter =>      NVL( p_delimiter, c_configs.delimiter ),
                               p_quotechar =>      NVL( p_quotechar, c_configs.quotechar ),
                               p_headers =>        NVL( p_headers, l_headers ));
               job.log_msg( 'Moving stage file to ' || l_dirname );
               UTL_FILE.frename( l_stgdirname,
                                 l_file,
                                 l_dirname,
                                 l_file,
                                 TRUE );
            WHEN NOT p_debug
            AND c_configs.stgdirname IS NULL
            THEN
               extract_object( p_owner =>          NVL( p_owner, c_configs.owner ),
                               p_object =>         NVL( p_object, c_configs.OBJECT ),
                               p_dirname =>        l_dirname,
                               p_filename =>       l_file,
                               p_delimiter =>      NVL( p_delimiter, c_configs.delimiter ),
                               p_quotechar =>      NVL( p_quotechar, c_configs.quotechar ),
                               p_headers =>        NVL( p_headers, l_headers ));
            WHEN p_debug
            AND c_configs.stgdirname IS NULL
            THEN
               job.log_msg( 'Data would be extracted to ' || l_dirname );
               extract_object( p_owner =>          NVL( p_owner, c_configs.owner ),
                               p_object =>         NVL( p_object, c_configs.OBJECT ),
                               p_dirname =>        l_dirname,
                               p_filename =>       l_file,
                               p_delimiter =>      NVL( p_delimiter, c_configs.delimiter ),
                               p_quotechar =>      NVL( p_quotechar, c_configs.quotechar ),
                               p_headers =>        NVL( p_headers, l_headers ));
            ELSE
               NULL;
         END CASE;

         IF p_debug
         THEN
            DBMS_OUTPUT.put_line(    'Extract SQL: '
                                  || 'select * from '
                                  || NVL( p_owner, c_configs.owner )
                                  || '.'
                                  || NVL( p_object, c_configs.OBJECT ));
         ELSE
            l_numlines := get_numlines;
            l_app.set_action( 'Checking number of lines' );
         END IF;

         CASE
            WHEN l_numlines > 65536
            THEN
               l_msg :=
                     '************'
                  || CHR( 13 )
                  || 'NOTE: Today''s '
                  || c_configs.EXTRACT
                  || ' file has '
                  || l_numlines
                  || ' rows'
                  || CHR( 13 )
                  || '      Excel is not capable of opening files over 65536 rows'
                  || CHR( 13 )
                  || '************';
            WHEN l_numlines = 0
            THEN
               l_msg :=
                     '************'
                  || CHR( 13 )
                  || 'NOTE: Today''s '
                  || c_configs.EXTRACT
                  || ' file is empty'
                  || CHR( 13 )
                  || '************';
            ELSE
               l_msg := NULL;
         END CASE;

         l_msg := l_msg || CHR( 13 ) || CHR( 13 ) || l_url;
         l_subject := c_configs.EXTRACT || ' Report URL' || CHR( 13 );

         CASE
            WHEN l_sendmail
            AND NOT p_debug
            THEN
               job.log_msg(    'Sending email to '
                            || NVL( p_recipients, c_configs.recipients )
                            || ' from '
                            || c_configs.sender );
               --                utl_mail.send (sender          => c_configs.sender,
               --                               recipients      => NVL (p_recipients, c_configs.recipients),
               --                               subject         => l_subject,
               --                               message         => l_msg,
               --                               mime_type       => 'text/html');
               utility.send_email( p_sender =>          c_configs.sender,
                                   p_recipients =>      NVL( p_recipients, c_configs.recipients ),
                                   p_subject =>         l_subject,
                                   p_message =>         l_msg );
               l_app.set_action( 'Formatting email message' );
            WHEN p_debug
            AND l_sendmail
            THEN
               DBMS_OUTPUT.put_line( 'Email Subject: ' || l_subject );
               DBMS_OUTPUT.put_line( 'Email MSG: ' || l_msg );
               DBMS_OUTPUT.put_line( 'Email URL: ' || l_url );
            ELSE
               NULL;
         END CASE;

         CASE
            WHEN l_arcdirname IS NOT NULL
            AND NOT p_debug
            THEN
               UTL_FILE.fcopy( l_dirname,
                               l_file,
                               l_arcdirname,
                               l_file );
            WHEN l_arcdirname IS NOT NULL
            AND p_debug
            THEN
               job.log_msg( l_file || ' would be copied to ' || l_arcdirname );
            ELSE
               NULL;
         END CASE;
      END LOOP;

      IF NOT l_rows
      THEN
         raise_application_error( -20001,
                                  'Combination of parameters yields no object to extract from.' );
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END process_extract;

   -- configure an extract
   PROCEDURE register_extract(
      p_extract_number   extract_conf.extract_number%TYPE DEFAULT NULL,
      p_extract          extract_conf.EXTRACT%TYPE,
      -- The name of the report to generate. This is the PK for the table.
      p_object           extract_conf.OBJECT%TYPE DEFAULT NULL,
      -- The name of the object to extract: a table or view typically.
      p_owner            extract_conf.owner%TYPE DEFAULT NULL,           -- The owner of the object.
      p_filebase         extract_conf.filebase%TYPE DEFAULT NULL,
      -- Basename of the extract file... minus the datastamp and file extension.
      p_filext           extract_conf.filext%TYPE DEFAULT NULL,
      -- Extension to place at the end of a file
      p_datestamp        extract_conf.datestamp%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for the file datestamp
      p_dateformat       extract_conf.DATEFORMAT%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for any date columns in the file
      p_dirname          extract_conf.dirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to stage the file in initially
      p_stgdirname       extract_conf.stgdirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to extract to.
      p_delimiter        extract_conf.delimiter%TYPE DEFAULT NULL,
      -- Column delimiter in the extract file.
      p_quotechar        extract_conf.quotechar%TYPE DEFAULT NULL,
      -- Character (if any) to use to quote columns.
      p_sender           extract_conf.sender%TYPE DEFAULT NULL,
      -- Character (if any) to use to quote columns.
      p_recipients       extract_conf.recipients%TYPE DEFAULT NULL,
      -- comma separated list of recipients
      p_baseurl          extract_conf.baseurl%TYPE DEFAULT NULL,
      -- URL (minus filename) of the link to the file
      p_headers          extract_conf.headers%TYPE DEFAULT NULL,
      -- whether to include headers in the file
      p_sendmail         extract_conf.sendmail%TYPE DEFAULT NULL,
      -- whether to send an email announcing the link
      p_arcdirname       extract_conf.arcdirname%TYPE DEFAULT NULL,
      p_debug            BOOLEAN DEFAULT FALSE )
   IS
      r_extract_conf   extract_conf%ROWTYPE;
      l_path           all_directories.directory_path%TYPE;
      l_app            app_info
                         := app_info( p_module =>      'EXTRACTS.REGISTER_EXTRACT',
                                      p_debug =>       p_debug );
   BEGIN
      SELECT NVL( p_extract, EXTRACT ),
	     extract_number,
             NVL( p_object, OBJECT ),
             NVL( p_owner, owner ),
             NVL( p_filebase, filebase ),
             NVL( p_filext, filext ),
             NVL( p_datestamp, datestamp ),
             NVL( p_dateformat, DATEFORMAT ),
             NVL( p_dirname, dirname ),
             NVL( p_stgdirname, stgdirname ),
             NVL( p_delimiter, delimiter ),
             NVL( p_quotechar, quotechar ),
             NVL( p_sender, sender ),
             NVL( p_recipients, recipients ),
             NVL( p_baseurl, baseurl ),
             NVL( p_headers, headers ),
             NVL( p_sendmail, sendmail ),
             NVL( p_arcdirname, arcdirname ),
             created_user,
             created_dt,
             SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             SYSDATE
        INTO r_extract_conf
        FROM extract_conf
       WHERE (    EXTRACT = p_extract
               OR extract_number = p_extract_number );

      -- make sure the directory names are legitimate
      -- the function raises and error if they aren't
      l_path := utility.get_dir_path( p_dirname );

      IF UPPER( p_arcdirname ) <> 'NA'
      THEN
         l_path := utility.get_dir_path( p_arcdirname );
      END IF;

      -- update if this is a modification of an extract
      UPDATE extract_conf
         SET EXTRACT = r_extract_conf.EXTRACT,
             OBJECT = r_extract_conf.OBJECT,
             owner = r_extract_conf.owner,
             filebase = r_extract_conf.filebase,
             filext = r_extract_conf.filext,
             datestamp = r_extract_conf.datestamp,
             DATEFORMAT = r_extract_conf.DATEFORMAT,
             dirname = r_extract_conf.dirname,
             stgdirname = r_extract_conf.stgdirname,
             delimiter = r_extract_conf.delimiter,
             quotechar = r_extract_conf.quotechar,
             sender = r_extract_conf.sender,
             recipients = r_extract_conf.recipients,
             baseurl = r_extract_conf.baseurl,
             headers = r_extract_conf.headers,
             sendmail = r_extract_conf.sendmail,
             arcdirname = r_extract_conf.arcdirname,
             modified_user = r_extract_conf.modified_user,
             modified_dt = r_extract_conf.modified_dt
       WHERE extract_number = r_extract_conf.extract_number;

      l_app.clear_app_info;
   EXCEPTION
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error( -20001, 'Invalid combination of parameters' );
      WHEN NO_DATA_FOUND
      THEN
         -- the particular extract does not exist in the table yet
         -- create it now
         INSERT INTO extract_conf
                     ( EXTRACT,
                       extract_number,
                       OBJECT,
                       owner,
                       filebase,
                       filext,
                       datestamp,
                       DATEFORMAT,
                       dirname,
                       delimiter,
                       quotechar,
                       sender,
                       recipients,
                       baseurl,
                       headers,
                       sendmail,
                       arcdirname,
                       created_user,
                       created_dt )
              VALUES ( p_extract,
                       extract_conf_seq.NEXTVAL,
                       UPPER( p_object ),
                       UPPER( p_owner ),
                       p_filebase,
                       p_filext,
                       p_datestamp,
                       p_DATEFORMAT,
                       UPPER( p_dirname ),
                       p_delimiter,
                       p_quotechar,
                       p_sender,
                       p_recipients,
                       p_baseurl,
                       p_headers,
                       p_sendmail,
                       UPPER( p_arcdirname ),
                       SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                       sysdate );

         l_app.clear_app_info;
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END register_extract;
END file_extract;
/