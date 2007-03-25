CREATE OR REPLACE TYPE BODY tdinc.EXTRACT
AS
   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   MEMBER FUNCTION extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE)
      RETURN NUMBER
   AS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2 (2000);
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2 (5)       DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
      l_mode          VARCHAR2 (1)       DEFAULT 'w';
      l_exists        BOOLEAN;
      l_length        NUMBER;
      l_blocksize     NUMBER;
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_var, -1007);
      o_app           applog             := applog (p_module => 'EXTRACTS.EXTRACT_QUERY');
   BEGIN
      IF p_append
      THEN
         l_mode := 'a';
      END IF;

      l_output := UTL_FILE.fopen (p_dirname, p_filename, l_mode, 32767);
      DBMS_SQL.parse (l_thecursor, p_query, DBMS_SQL.native);
      o_app.set_action ('Open Cursor to define columns');

      FOR i IN 1 .. 255
      LOOP
         BEGIN
            DBMS_SQL.define_column (l_thecursor, i, l_columnvalue, 2000);
            l_colcnt := i;
         EXCEPTION
            WHEN e_no_var
            THEN
               EXIT;
         END;
      END LOOP;

      DBMS_SQL.define_column (l_thecursor, 1, l_columnvalue, 2000);
      l_status := DBMS_SQL.EXECUTE (l_thecursor);
      o_app.set_action ('Open Cursor to pull back records');

      LOOP
         EXIT WHEN (DBMS_SQL.fetch_rows (l_thecursor) <= 0);
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE (l_thecursor, i, l_columnvalue);
            UTL_FILE.put (l_output, l_delimiter || p_quotechar || l_columnvalue || p_quotechar);
            l_delimiter := p_delimiter;
         END LOOP;

         UTL_FILE.new_line (l_output);
         l_cnt := l_cnt + 1;
      END LOOP;

      o_app.set_action ('Close DBMS_SQL cursor and filehandles.');
      DBMS_SQL.close_cursor (l_thecursor);
      UTL_FILE.fclose (l_output);
      o_app.clear_app_info;
      RETURN l_cnt;
   END extract_query;
   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   MEMBER FUNCTION extract_object (
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'N',
      p_append      BOOLEAN DEFAULT FALSE)
      RETURN NUMBER
   IS
      l_cnt           NUMBER          := 0;
      l_head_sql      VARCHAR (1000);
      l_extract_sql   VARCHAR2 (1000);
      o_app           applog
                      := applog (p_module      => 'EXTRACTS.EXTRACT_OBJECT',
                                 p_debug       => SELF.DEBUG_MODE);
   BEGIN
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
         || UPPER (p_object)
         || ''' and owner='''
         || UPPER (p_owner)
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;

      IF SELF.DEBUG_MODE
      THEN
         o_app.log_msg ('Headers query: ' || l_head_sql);
         o_app.log_msg ('Extract query: ' || l_extract_sql);
      ELSE
         IF p_headers = 'Y'
         THEN
            o_app.set_action ('Extract headers to file');
            l_cnt :=
               extract_query (p_query          => l_head_sql,
                              p_dirname        => p_dirname,
                              p_filename       => p_filename,
                              p_delimiter      => p_delimiter,
                              p_quotechar      => NULL,
                              p_append         => p_append);
         END IF;

         o_app.set_action ('Extract date to file');
         l_cnt :=
              l_cnt
            + extract_query (p_query          => l_extract_sql,
                             p_dirname        => p_dirname,
                             p_filename       => p_filename,
                             p_delimiter      => p_delimiter,
                             p_quotechar      => p_quotechar,
                             p_append         => CASE
                                WHEN p_headers = 'Y'
                                   THEN TRUE
                                ELSE p_append
                             END);
      END IF;

      o_app.clear_app_info;
      RETURN l_cnt;
   END extract_object;
   -- extract data to a text file, and then peform other functions as defined in the configuration table
   MEMBER PROCEDURE process_extract
   AS
      l_num_bytes   NUMBER;
      l_numlines    NUMBER;
      l_blocksize   NUMBER;
      l_exists      BOOLEAN DEFAULT FALSE;
      l_file_dt     DATE;
      l_detail_id   NUMBER;
      o_app         applog
                     := applog (p_module      => 'EXTRACTS.PROCESS_EXTRACT',
                                p_debug       => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Configure NLS formats');
      -- set date and timestamp NLS formats
      coreutils.ddl_exec (dateformat_ddl, 'nls_date_format DDL: ', SELF.DEBUG_MODE);
      coreutils.ddl_exec (tsformat_ddl, 'nls_timestamp_format DDL: ', SELF.DEBUG_MODE);
      o_app.set_action ('Extract data');
      -- extract data to arch location first
      l_numlines :=
         extract_object (p_owner          => object_owner,
                         p_object         => object_name,
                         p_dirname        => arch_directory,
                         p_filename       => arch_filename,
                         p_delimiter      => delimiter,
                         p_quotechar      => quotechar,
                         p_headers        => headers);
      o_app.log_msg (   l_numlines
                     || ' '
                     || CASE l_numlines
                           WHEN 1
                              THEN 'row'
                           ELSE 'rows'
                        END
                     || ' extracted to '
                     || arch_filepath);
      l_file_dt := SYSDATE;
      -- copy the file to the target location
      coreutils.copy_file (arch_filepath, filepath, SELF.DEBUG_MODE);

      -- get file attributes
      IF SELF.DEBUG_MODE
      THEN
         l_num_bytes := 0;
         o_app.log_msg ('Reporting 0 size file in debug mode');
      ELSE
         UTL_FILE.fgetattr (DIRECTORY, filename, l_exists, l_num_bytes, l_blocksize);
      END IF;

      -- audit the file
      o_app.set_action ('Audit extract file');
      SELF.audit_file (p_num_bytes      => l_num_bytes, p_num_lines => l_numlines,
                       p_file_dt        => l_file_dt);
      -- send the notification if configured
      o_app.set_action ('Send a notification');
      MESSAGE :=
            MESSAGE
         || CHR (10)
         || CHR (10)
         || 'The file can be downloaded at the following link:'
         || CHR (10)
         || file_url;

      IF l_numlines > 65536
      THEN
         MESSAGE :=
               MESSAGE
            || CHR (10)
            || CHR (10)
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      SELF.send;
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END process_extract;
END;
/