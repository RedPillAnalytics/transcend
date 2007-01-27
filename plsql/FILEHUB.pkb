CREATE OR REPLACE PACKAGE BODY efw.filehub
IS
   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query
      ( p_query       VARCHAR2,
	p_dirname     VARCHAR2,
	p_filename    VARCHAR2,
	p_delimiter   VARCHAR2 DEFAULT ',',
	p_quotechar   VARCHAR2 DEFAULT '',
	p_append      BOOLEAN DEFAULT FALSE )
      RETURN number
   IS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2( 2000 );
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2( 5 )      DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
      l_mode          VARCHAR2( 1 )      DEFAULT 'w';
      l_exists BOOLEAN;
      l_length NUMBER;
      l_blocksize NUMBER;
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_var, -1007 );
      o_app           applog           := applog( p_module =>      'EXTRACTS.EXTRACT_QUERY' );
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
      o_app.set_action( 'Open Cursor to define columns' );

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
      o_app.set_action( 'Open Cursor to pull back records' );

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
            
      job.log_msg( l_cnt || ' rows extracted to ' || p_filename );
      o_app.set_action( 'Close DBMS_SQL cursor and filehandles.' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );      

      o_app.clear_app_info;
      RETURN l_cnt;
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
      o_app   applog
         := applog( p_module =>      'EXTRACTS.EXTRACT_OBJECT',
                      p_action =>      'Extract headers using EXTRACT_QUERY' );
   BEGIN
      IF p_headers
      THEN
         extract_query(    'select regexp_replace(stragg(column_name),'','','''
                        || p_delimiter
                        || ''') from '
                        || '(select '
			|| p_quotechar
			|| '||column_name||'
			|| p_quotechar
			|| ' from all_tab_cols '
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

      o_app.set_action( 'Extract data using EXTRACT_QUERY' );
      extract_query( 'select * from ' || p_owner || '.' || p_object,
                     p_dirname,
                     p_filename,
                     p_delimiter,
                     p_quotechar,
                     TRUE );
      o_app.clear_app_info;
   END extract_object;

   -- audits information about feeds and extracts to the FILEHUB_DTL table
   PROCEDURE audit_file 
      ( p_filehub_id filehub_dtl.filehub_id%type,
	p_src_filename filehub_dtl.src_filename%type,
	p_trg_filename filehub_dtl.trg_filename%type,
	p_arch_filename filehub_dtl.arch_filename%type,
	p_num_bytes filehub_dtl.num_bytes%type,
	p_file_dt filehub_dtl.file_dt%type,
	p_filehub_type filehub_dtl.filehub_type%type,
	p_debug BOOLEAN DEFAULT FALSE )
   AS
      r_fh_conf   filehub_conf%ROWTYPE;
      o_app        applog   := applog (p_module      => 'FILE_MOVER.AUDIT_FILE',
                                           p_debug       => p_debug);
   BEGIN
      OPEN c_proc_extract;
      FETCH c_proc_extract INTO r_proc_extract;

      o_app.set_action ('Insert FILE_DTL');

      -- INSERT into the FILE_DTL table to record the movement
      INSERT INTO filehub_detail
             ( filehub_detail_id, 
               src_filename, 
               trg_filename, 
               arch_filename, 
               filehub_type, 
               jobname, 
               filehub_id, 
               num_bytes, 
               num_lines, 
               file_dt, 
               processed_ts,
	       ext_tab_process, 
               session_id)
             VALUES ( filehub_dtl_seq.nextval,
		      p_src_filename,
		      p_trg_filename,
                      p_arch_filename,
		      p_filehub_type,
                      r_proc_extract.jobname,
		      r_proc_extract.file_process_id
                      p_num_bytes,
                      p_file_dt,
                      CURRENT_TIMESTAMP,
                      SYS_CONTEXT ('USERENV', 'SESSIONID'),
                      p_jobnumber,
                      'N',
		      CASE 
		      WHEN REGEXP_SUBSTR (r_fh_conf.multi_files_action, '^All$', 1, 1, 'i') IS NOT NULL THEN 'Y'
			     ELSE
			     'N'
			     END);

      -- IF the size threshholds are not met, then fail the job
      -- ALL the copies occur successfully, but nothing else happens
      o_app.set_action ('Check file details');

      IF p_num_bytes >= r_proc_extract.max_bytes AND r_proc_extract.max_bytes <> 0
      THEN
         raise_application_error (-20001, 'File size larger than MAX_BYTES');
      ELSIF p_num_bytes < r_proc_extract.min_bytes
      THEN
         raise_application_error (-20001, 'File size smaller than MIN_BYTES');
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