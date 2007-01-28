CREATE OR REPLACE PACKAGE BODY tdinc.filehub
IS
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
      job.log_msg ('Extracting data to ' || p_filename || ' in directory ' || p_dirname);
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

      job.log_msg (l_cnt || ' rows extracted to ' || p_filename);
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
      p_append      BOOLEAN DEFAULT FALSE,
      p_headers     BOOLEAN DEFAULT FALSE)
      RETURN NUMBER
   IS
      l_cnt   NUMBER := 0;
      o_app   applog
         := applog (p_module      => 'EXTRACTS.EXTRACT_OBJECT',
                    p_action      => 'Extract headers using EXTRACT_QUERY');
   BEGIN
      IF p_headers
      THEN
         l_cnt :=
            extract_query (p_query          =>    'select regexp_replace(stragg(column_name),'','','''
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
                                               || ''' order by column_id)',
                           p_dirname        => p_dirname,
                           p_filename       => p_filename,
                           p_delimiter      => p_delimiter,
                           p_quotechar      => NULL,
                           p_append         => p_append);
      END IF;

      o_app.set_action ('Extract data using EXTRACT_QUERY');
      l_cnt :=
           l_cnt
         + extract_query (p_query          => 'select * from ' || p_owner || '.' || p_object,
                          p_dirname        => p_dirname,
                          p_filename       => p_filename,
                          p_delimiter      => p_delimiter,
                          p_quotechar      => p_quotechar,
                          p_append         => CASE
                             WHEN p_headers
                                THEN TRUE
                             ELSE p_append
                          END);
      o_app.clear_app_info;
      RETURN l_cnt;
   END extract_object;

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
         job.log_err;
         RAISE;
   END audit_file;
END filehub;
/