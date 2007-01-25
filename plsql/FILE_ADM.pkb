CREATE OR REPLACE PACKAGE BODY efw.file_adm
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
            
      job.log_msg( l_cnt || ' rows extracted to ' || p_filename );
      l_app.set_action( 'Close DBMS_SQL cursor and filehandles.' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );      

      l_app.clear_app_info;
      RETURN l_cnt;
   END extract_query;

   -- writes information in the FILE_DTL table about files found in SOURCE_DIR
   -- SOURCE_DIR is configured in the FILE_CTL table
   PROCEDURE audit_file 
      ( p_file_proc_id file_process_dtl.file_process_id%type,
	p_src_filename file_process_dtl.src_filename%type,
	p_trg_filename file_process_dtl.trg_filename%type,
	p_arch_filename file_process_dtl.arch_filename%type,
	p_num_bytes file_process_dtl.num_bytes%type,
	p_file_dt file_process_dtl.file_dt%type,
	p_process_type file_process_dtl.process_type%type,
	p_debug BOOLEAN DEFAULT FALSE )
   AS
      CURSOR c_proc_extract IS
	     SELECT *
	       FROM file_process_conf
	       JOIN file_extract_conf USING (file_process_id)
	      WHERE file_process_id = p_file_proc_id;
      r_proc_extract   c_proc_extract%ROWTYPE;
      l_app        app_info   := app_info (p_module      => 'FILE_MOVER.AUDIT_FILE',
                                           p_debug       => p_debug);
   BEGIN
      OPEN c_proc_extract;
      FETCH c_proc_extract INTO r_proc_extract;

      l_app.set_action ('Insert FILE_DTL');

      -- INSERT into the FILE_DTL table to record the movement
      INSERT INTO file_process_dtl
             ( file_dtl_id,
	       src_filename,
               trg_filename,
               arch_filename,
	       src_filename,
               jobname,
               num_bytes,
               file_dt,
               processed_ts,
               session_id,
               jobnumber,
               ext_tab_ind,
               alt_ext_tab_ind)
             VALUES ( file_process_dtl_seq.nextval,
		      p_filename,
                      p_archfilename,
                      r_proc_extract.jobname,
                      p_num_bytes,
                      p_file_dt,
                      CURRENT_TIMESTAMP,
                      SYS_CONTEXT ('USERENV', 'SESSIONID'),
                      p_jobnumber,
                      'N',
		      CASE 
		      WHEN REGEXP_SUBSTR (r_proc_extract.multi_files_action, '^All$', 1, 1, 'i') IS NOT NULL THEN 'Y'
			     ELSE
			     'N'
			     END);

      -- IF the size threshholds are not met, then fail the job
      -- ALL the copies occur successfully, but nothing else happens
      l_app.set_action ('Check file details');

      IF p_num_bytes >= r_proc_extract.max_bytes AND r_proc_extract.max_bytes <> 0
      THEN
         raise_application_error (-20001, 'File size larger than MAX_BYTES');
      ELSIF p_num_bytes < r_proc_extract.min_bytes
      THEN
         raise_application_error (-20001, 'File size smaller than MIN_BYTES');
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END audit_file;

END file_adm;
/