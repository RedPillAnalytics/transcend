CREATE OR REPLACE TYPE BODY EXTRACT
AS
   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   MEMBER FUNCTION extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      varchar2 DEFAULT 'no')
      RETURN NUMBER
   AS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2 (2000);
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2 (5)       DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
l_mode          VARCHAR2 (1):= CASE lower(p_append) WHEN 'yes' THEN 'a' ELSE 'w' END;
      l_exists        BOOLEAN;
      l_length        NUMBER;
      l_blocksize     NUMBER;
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_var, -1007);
      o_app           applog             := applog (p_module => 'extract_query');
   BEGIN
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
            l_delimiter := delimiter;
         END LOOP;

         UTL_FILE.new_line (l_output);
         l_cnt := l_cnt + 1;
      END LOOP;

      o_app.set_action ('Close cursor and handles');
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
      p_append      varchar2 DEFAULT 'no')
      RETURN NUMBER
   IS
      l_cnt           NUMBER          := 0;
      l_head_sql      VARCHAR (1000);
      l_extract_sql   VARCHAR2 (1000);
      o_app           applog
                      := applog (p_module      => 'extract_object',
                                 p_runmode       => SELF.runmode);
   BEGIN
      l_head_sql :=
            'select regexp_replace(stragg(column_name),'','','''
         || delimiter
         || ''') from '
         || '(select '''
         || quotechar
         || '''||column_name||'''
         || quotechar
         || ''' as column_name'
         || ' from all_tab_cols '
         || 'where table_name='''
         || UPPER (p_object)
         || ''' and owner='''
         || UPPER (p_owner)
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;

      o_app.log_msg ('Headers query: ' || l_head_sql,3);
      o_app.log_msg ('Extract query: ' || l_extract_sql,3);


      IF NOT SELF.is_debugmode
      THEN
         IF headers = 'yes'
         THEN
            o_app.set_action ('Extract headers to file');
            l_cnt :=
               extract_query (p_query          => l_head_sql,
                              p_dirname        => p_dirname,
                              p_filename       => p_filename,
                              p_quotechar      => NULL,
                              p_append         => p_append);
         END IF;

         o_app.set_action ('Extract data to file');
         l_cnt :=
              l_cnt
            + extract_query (p_query          => l_extract_sql,
                             p_dirname        => p_dirname,
                             p_filename       => p_filename,
                             p_quotechar      => quotechar,
                             p_append         => p_append);
      END IF;

      o_app.clear_app_info;
      RETURN l_cnt;
   END extract_object;
   -- extract data to a text file, and then peform other functions as defined in the configuration table
   MEMBER PROCEDURE process
   AS
      l_num_bytes   NUMBER;
      l_numlines    NUMBER;
      l_blocksize   NUMBER;
      l_exists      BOOLEAN                    DEFAULT FALSE;
      l_file_dt     DATE;
      l_detail_id   NUMBER;
      l_message     notify_conf.MESSAGE%TYPE;
      o_app         applog    := applog (p_module      => 'process',
                                         p_runmode       => runmode);
   BEGIN
      o_app.set_action ('Configure NLS formats');
      -- set date and timestamp NLS formats
      td_core.exec_sql (dateformat_ddl, runmode, 'nls_date_format DDL: ');
      td_core.exec_sql (tsformat_ddl, runmode, 'nls_timestamp_format DDL: ');
      o_app.set_action ('Extract data');
      -- extract data to arch location first
      l_numlines :=
         extract_object (p_owner          => object_owner,
                         p_object         => object_name,
                         p_dirname        => arch_directory,
                         p_filename       => arch_filename);
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
      td_core.copy_file (arch_filepath, filepath, SELF.runmode);

      -- get file attributes
      IF SELF.is_debugmode
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
      o_app.set_action ('Notify success');
      l_message := 'The file can be downloaded at the following link:' || CHR (10) || file_url;

      IF l_numlines > 65536
      THEN
         l_message :=
               l_message
            || CHR (10)
            || CHR (10)
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      o_app.send (p_module_id =>filehub_id, p_message => l_message);
      o_app.clear_app_info;
   END process;
END;
/