CREATE OR REPLACE PACKAGE BODY tdinc.filehub
IS
   -- audits information about feeds and extracts to the FILEHUB_DTL table
   PROCEDURE audit_file (
      p_filehub_id      filehub_detail.filehub_id%TYPE,
      p_src_filename    filehub_detail.src_filename%TYPE DEFAULT NULL,
      p_trg_filename    filehub_detail.trg_filename%TYPE DEFAULT NULL,
      p_arch_filename   filehub_detail.arch_filename%TYPE,
      p_num_bytes       filehub_detail.num_bytes%TYPE,
      p_num_lines       filehub_detail.num_lines%TYPE DEFAULT NULL,
      p_file_dt         filehub_detail.file_dt%TYPE,
      p_debug           BOOLEAN DEFAULT FALSE)
   AS
      r_fh_conf   filehub_conf%ROWTYPE;
      o_app       applog        := applog (p_module      => 'FILE_MOVER.AUDIT_FILE',
                                           p_debug       => p_debug);
   BEGIN
      SELECT *
        INTO r_fh_conf
        FROM filehub_conf
       WHERE filehub_id = p_filehub_id;

      o_app.set_action ('Insert FILE_DTL');

      -- INSERT into the FILE_DTL table to record the movement
      INSERT INTO filehub_detail
                  (fh_detail_id,
                   filehub_id,
                   filehub_name,
                   filehub_group,
                   filehub_type,
                   src_filename,
                   trg_filename,
                   arch_filename,
                   num_bytes,
                   num_lines,
                   file_dt)
           VALUES (filehub_detail_seq.NEXTVAL,
                   p_filehub_id,
                   r_fh_conf.filehub_name,
                   r_fh_conf.filehub_group,
                   r_fh_conf.filehub_type,
                   p_src_filename,
                   p_trg_filename,
                   p_arch_filename,
                   p_num_bytes,
                   p_num_lines,
                   p_file_dt);

      -- the job fails when size threshholds are not met
      o_app.set_action ('Check file details');

      IF p_num_bytes >= r_fh_conf.max_bytes AND r_fh_conf.max_bytes <> 0
      THEN
         raise_application_error (-20015, 'File size larger than MAX_BYTES paramter');
      ELSIF p_num_bytes < r_fh_conf.min_bytes
      THEN
         raise_application_error (-20016, 'File size smaller than MIN_BYTES parameter');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END audit_file;

   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE)
      RETURN NUMBER
   IS
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
      o_app.log_msg ('Extracting data to ' || p_filename || ' in directory ' || p_dirname);
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

      o_app.log_msg (l_cnt || ' rows extracted to ' || p_filename);
      o_app.set_action ('Close DBMS_SQL cursor and filehandles.');
      DBMS_SQL.close_cursor (l_thecursor);
      UTL_FILE.fclose (l_output);
      o_app.clear_app_info;
      RETURN l_cnt;
   END extract_query;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   FUNCTION extract_object (
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'none',
      p_append      BOOLEAN DEFAULT FALSE,
      p_debug       BOOLEAN DEFAULT FALSE)
      RETURN NUMBER
   IS
      l_cnt           NUMBER          := 0;
      l_head_sql      VARCHAR (1000);
      l_extract_sql   VARCHAR2 (1000);
      o_app           applog  := applog (p_module      => 'EXTRACTS.EXTRACT_OBJECT',
                                         p_debug       => p_debug);
   BEGIN
      l_head_sql :=
            'select regexp_replace(stragg(column_name),'','','''
         || p_delimiter
         || ''') from '
         || '(select '
         || p_quotechar
         || '||column_name||'
         || p_quotechar
         || ' from all_tab_cols '
         || 'where table_name='''
         || UPPER (p_object)
         || ''' and owner='''
         || UPPER (p_owner)
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;

      IF p_debug
      THEN
         o_app.log_msg ('Headers query: ' || l_head_sql);
         o_app.log_msg ('Extract query: ' || l_extract_sql);
      ELSE
         IF p_headers = 'include'
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
                                WHEN p_headers = 'include'
                                   THEN TRUE
                                ELSE p_append
                             END);
      END IF;

      o_app.clear_app_info;
      RETURN l_cnt;
   END extract_object;

   -- extract data to a text file, and then peform other functions as defined in the configuration table
   PROCEDURE process_extract (
      p_filehub_id        filehub_conf.filehub_id%TYPE DEFAULT NULL,
      p_object_owner      filehub_conf.object_owner%TYPE DEFAULT NULL,
      p_object_name       filehub_conf.object_name%TYPE DEFAULT NULL,
      p_directory         filehub_conf.DIRECTORY%TYPE DEFAULT NULL,
      p_filename          filehub_conf.filename%TYPE DEFAULT NULL,
      p_arch_directory    filehub_conf.arch_directory%TYPE DEFAULT NULL,
      p_min_bytes         filehub_conf.min_bytes%TYPE DEFAULT NULL,
      p_max_bytes         filehub_conf.max_bytes%TYPE DEFAULT NULL,
      p_file_datestamp    filehub_conf.file_datestamp%TYPE DEFAULT NULL,
      p_dateformat        filehub_conf.DATEFORMAT%TYPE DEFAULT NULL,
      p_timestampformat   filehub_conf.timestampformat%TYPE DEFAULT NULL,
      p_notification      filehub_conf.notification%TYPE DEFAULT NULL,
      p_delimiter         filehub_conf.delimiter%TYPE DEFAULT NULL,
      p_quotechar         filehub_conf.quotechar%TYPE DEFAULT NULL,
      p_headers           filehub_conf.headers%TYPE DEFAULT NULL,
      p_debug             BOOLEAN DEFAULT FALSE)
   AS
      TYPE t_fh_conf IS RECORD (
         arch_directory        filehub_conf.DIRECTORY%TYPE,
         arch_filename         filehub_conf.filename%TYPE,
         filename              filehub_conf.filename%TYPE,
         DIRECTORY             filehub_conf.DIRECTORY%TYPE,
         filepath              VARCHAR2 (100),
         arch_filepath         VARCHAR2 (100),
         source_owner          filehub_conf.object_owner%TYPE,
         source_object         filehub_conf.object_name%TYPE,
         min_bytes             filehub_conf.min_bytes%TYPE,
         max_bytes             filehub_conf.max_bytes%TYPE,
         notification          filehub_conf.notification%TYPE,
         dateformat_ddl        VARCHAR2 (200),
         timestampformat_ddl   VARCHAR2 (200),
         delimiter             filehub_conf.delimiter%TYPE,
         quotechar             filehub_conf.quotechar%TYPE,
         headers               filehub_conf.headers%TYPE
      );

      r_fh_conf     t_fh_conf;
      l_num_bytes   NUMBER;
      l_numlines    NUMBER;
      l_blocksize   NUMBER;
      l_exists      BOOLEAN   DEFAULT FALSE;
      l_file_dt     DATE;
      o_app         applog    := applog (p_module      => 'EXTRACTS.PROCESS_EXTRACT',
                                         p_debug       => p_debug);
   BEGIN
      BEGIN
         SELECT arch_directory,
                arch_filename,
                filename,
                DIRECTORY,
                coreutils.get_dir_path (DIRECTORY) || '/' || filename filepath,
                coreutils.get_dir_path (arch_directory) || '/' || arch_filename arch_filepath,
                source_owner,
                source_object,
                min_bytes,
                max_bytes,
                notification,
                'alter session set nls_date_format=''' || DATEFORMAT || '''' dateformat_ddl,
                'alter session set nls_date_format=''' || timestampformat || ''''
                                                                                timestampformat_ddl,
                delimiter,
                quotechar,
                headers
           INTO r_fh_conf
           FROM (SELECT NVL (p_filehub_id, filehub_id) filehub_id,
                        filehub_name filehub_name,
                        filehub_group filehub_group,
                        filehub_type,
                        UPPER (NVL (p_object_owner, object_owner)) source_owner,
                        UPPER (NVL (p_object_name, object_name)) source_object,
                        NVL (p_directory, DIRECTORY) DIRECTORY,
                        CASE NVL (p_file_datestamp, file_datestamp)
                           WHEN 'NA'
                              THEN NVL (p_filename, filename)
                           ELSE REGEXP_REPLACE (NVL (p_filename, filename),
                                                '\.',
                                                   '_'
                                                || TO_CHAR (SYSDATE,
                                                            NVL (p_file_datestamp, file_datestamp))
                                                || '.')
                        END filename,
                        CASE NVL (p_file_datestamp, file_datestamp)
                           WHEN 'NA'
                              THEN    NVL (p_filename, filename)
                                   || '.'
                                   || TO_CHAR (SYSDATE, 'yyyymmddhhmiss')
                           ELSE REGEXP_REPLACE (NVL (p_filename, filename),
                                                '\.',
                                                   '_'
                                                || TO_CHAR (SYSDATE,
                                                            NVL (p_file_datestamp, file_datestamp))
                                                || '.')
                        END arch_filename,
                        NVL (p_arch_directory, arch_directory) arch_directory,
                        NVL (p_min_bytes, min_bytes) min_bytes,
                        NVL (p_max_bytes, max_bytes) max_bytes,
                        NVL (p_notification, notification) notification,
                        NVL (p_dateformat, DATEFORMAT) DATEFORMAT,
                        NVL (p_timestampformat, timestampformat) timestampformat,
                        NVL (p_delimiter, delimiter) delimiter,
                        NVL (p_quotechar, quotechar) quotechar,
                        NVL (p_headers, headers) headers
                   FROM filehub_conf
                  WHERE filehub_id = filehub_id AND REGEXP_LIKE (filehub_type, '^extract$', 'i'));
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error (-20001, '');
      END;

      o_app.set_action ('Configure NLS formats');
      -- set date and timestamp NLS formats
      coreutils.ddl_exec (r_fh_conf.dateformat_ddl, 'nls_date_format DDL: ', p_debug);
      coreutils.ddl_exec (r_fh_conf.timestampformat_ddl, 'nls_timestamp_format DDL: ', p_debug);
      o_app.set_action ('Extract data');
      -- extract data to arch location first
      l_numlines :=
         extract_object (p_owner          => r_fh_conf.source_owner,
                         p_object         => r_fh_conf.source_object,
                         p_dirname        => r_fh_conf.arch_directory,
                         p_filename       => r_fh_conf.arch_filename,
                         p_delimiter      => r_fh_conf.delimiter,
                         p_quotechar      => r_fh_conf.quotechar,
                         p_headers        => r_fh_conf.headers,
                         p_debug          => p_debug);
      l_file_dt := SYSDATE;
      -- copy the file to the target location
      coreutils.host_cmd ('cp -p ' || r_fh_conf.arch_filepath || ' ' || r_fh_conf.filepath,
                           p_debug      => p_debug);

      -- get file attributes
      IF p_debug
      THEN
         l_num_bytes := 0;
         o_app.log_msg ('Reporting 0 size file in debug mode');
      ELSE
         UTL_FILE.fgetattr (r_fh_conf.DIRECTORY,
                            r_fh_conf.filename,
                            l_exists,
                            l_num_bytes,
                            l_blocksize);
      END IF;

      -- audit the file just extracted
      -- don't yet know how I'll get file_dt
      o_app.set_action ('Audit extract file');
      audit_file (p_filehub_id         => p_filehub_id,
                  p_trg_filename       => r_fh_conf.filepath,
                  p_arch_filename      => r_fh_conf.arch_filepath,
                  p_num_bytes          => l_num_bytes,
                  p_num_lines          => l_numlines,
                  p_file_dt            => l_file_dt,
                  p_debug              => p_debug);

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END process_extract;
END filehub;
/