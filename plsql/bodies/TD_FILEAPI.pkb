CREATE OR REPLACE PACKAGE BODY td_fileapi
IS
   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      VARCHAR2 DEFAULT 'no',
      p_runmode	    VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER
   AS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2( 2000 );
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2( 5 )      DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
      l_mode          VARCHAR2( 1 )      := CASE LOWER( p_append )
         WHEN 'yes'
            THEN 'a'
         ELSE 'w'
      END;
      l_exists        BOOLEAN;
      l_length        NUMBER;
      l_blocksize     NUMBER;
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_var, -1007 );
      o_td            tdtype             := tdtype( p_module => 'extract_query',
						    p_runmode => p_runmode );
   BEGIN
      l_output := UTL_FILE.fopen( p_dirname, p_filename, l_mode, 32767 );
      DBMS_SQL.parse( l_thecursor, p_query, DBMS_SQL.native );
      o_td.change_action( 'Open Cursor to define columns' );

      FOR i IN 1 .. 255
      LOOP
         BEGIN
            DBMS_SQL.define_column( l_thecursor, i, l_columnvalue, 2000 );
            l_colcnt := i;
         EXCEPTION
            WHEN e_no_var
            THEN
               EXIT;
         END;
      END LOOP;

      DBMS_SQL.define_column( l_thecursor, 1, l_columnvalue, 2000 );
      l_status := DBMS_SQL.EXECUTE( l_thecursor );
      o_td.change_action( 'Open Cursor to pull back records' );

      LOOP
         EXIT WHEN( DBMS_SQL.fetch_rows( l_thecursor ) <= 0 );
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE( l_thecursor, i, l_columnvalue );
	    IF NOT o_td.is_debugmode
	    THEN
               UTL_FILE.put( l_output,
                             l_delimiter || p_quotechar || l_columnvalue || p_quotechar
                           );
	    END IF;
            l_delimiter := p_delimiter;
         END LOOP;

         UTL_FILE.new_line( l_output );
         l_cnt := l_cnt + 1;
      END LOOP;

      o_td.change_action( 'Close cursor and handles' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );
      o_td.clear_app_info;
      RETURN l_cnt;
   END extract_query;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   FUNCTION extract_object(
      p_owner      VARCHAR2,
      p_object     VARCHAR2,
      p_dirname    VARCHAR2,
      p_filename   VARCHAR2,
      p_delimiter  VARCHAR2 DEFAULT '|',
      p_quotechar  VARCHAR2 DEFAULT '',
      p_headers    VARCHAR2 DEFAULT 'yes',
      p_append     VARCHAR2 DEFAULT 'no',
      p_runmode	   VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER
   IS
      l_cnt           NUMBER           := 0;
      l_head_sql      VARCHAR( 1000 );
      l_extract_sql   VARCHAR2( 1000 );
      o_td            tdtype
                     := tdtype( p_module       => 'extract_object',
                                p_runmode      => p_runmode );
   BEGIN
      -- check that the source object exists and is something we can select from
      td_sql.check_object( p_owner => p_owner, 
			   p_object => p_object,
			   p_object_type => 'table$|view');

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
         || UPPER( p_object )
         || ''' and owner='''
         || UPPER( p_owner )
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;
      o_td.log_msg( 'Headers query: ' || l_head_sql, 3 );
      o_td.log_msg( 'Extract query: ' || l_extract_sql, 3 );

      IF NOT o_td.is_debugmode
      THEN
         IF td_ext.is_true( p_headers )
         THEN
            o_td.change_action( 'Extract headers to file' );
            l_cnt :=
               extract_query( p_query          => l_head_sql,
                              p_dirname        => p_dirname,
                              p_filename       => p_filename,
                              p_delimiter      => p_delimiter,
			      p_quotechar      => NULL,
                              p_append         => p_append,
			      p_runmode	       => p_runmode
                            );
         END IF;

         o_td.change_action( 'Extract data to file' );
         l_cnt :=
              l_cnt
            + extract_query( p_query          => l_extract_sql,
                             p_dirname        => p_dirname,
                             p_filename       => p_filename,
                             p_delimiter      => p_delimiter,
                             p_quotechar      => p_quotechar,
                             p_append         => p_append,
			     p_runmode	      => p_runmode
                           );
      END IF;

      o_td.clear_app_info;
      RETURN l_cnt;
   END extract_object;

   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   FUNCTION calc_rej_ind(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2,
      p_rej_limit       NUMBER DEFAULT 20
   )
      RETURN VARCHAR2
   IS
      l_pct_diff   NUMBER;
      l_rej_ind    VARCHAR2( 1 );
      o_td         tdtype        := tdtype( p_module => 'calc_rej_ind' );
   BEGIN
      SELECT percent_diff
        INTO l_pct_diff
        FROM filehub_obj_detail
       WHERE filehub_group = p_filehub_group
         AND filehub_name = p_filehub_name
         AND processed_ts =
                ( SELECT MAX( processed_ts )
                   FROM filehub_obj_detail
                  WHERE filehub_group = p_filehub_group AND filehub_name = p_filehub_name );

      IF l_pct_diff > p_rej_limit
      THEN
         RETURN 'N';
      ELSE
         RETURN 'Y';
      END IF;

      o_td.clear_app_info;
   END calc_rej_ind;

   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   PROCEDURE process_files(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2 DEFAULT NULL,
      p_keep_source     VARCHAR2 DEFAULT 'no',
      p_runmode         VARCHAR2 DEFAULT NULL
   )
   IS
      l_rows      BOOLEAN := FALSE;                             -- TO catch empty cursors
      o_extract   extracttype;
      o_feed      feedtype;
      o_td        tdtype  := tdtype( p_module       => 'process_file',
                                     p_runmode      => p_runmode );
   BEGIN
      FOR c_fh_conf IN ( SELECT  filehub_id, filehub_type
                            FROM filehub_conf
                           WHERE filehub_group = p_filehub_group
                             AND REGEXP_LIKE( filehub_name,
                                              DECODE( p_filehub_name,
                                                      NULL, '?',
                                                      p_filehub_name
                                                    )
                                            )
                        ORDER BY filehub_id )
      LOOP
         l_rows := TRUE;

         CASE LOWER( c_fh_conf.filehub_type )
            WHEN 'extract'
            THEN
               SELECT VALUE( t )
                 INTO o_extract
                 FROM extract_ot t
                WHERE t.filehub_id = c_fh_conf.filehub_id;

               o_extract.runmode := o_td.runmode;
               o_extract.process;
            WHEN 'feed'
            THEN
               SELECT VALUE( t )
                 INTO o_feed
                 FROM feed_ot t
                WHERE t.filehub_id = c_fh_conf.filehub_id;

               o_feed.runmode := o_td.runmode;
               o_feed.process( p_keep_source );
            ELSE
               NULL;
         END CASE;

         -- need this commit to clear out the contents of the DIR_LIST table
         COMMIT;
      END LOOP;

      -- no matching filehub entries are found
      IF NOT l_rows
      THEN
         raise_application_error( td_ext.get_err_cd( 'incorrect_parameters' ),
                                  td_ext.get_err_msg( 'incorrect_parameters' )
                                );
      END IF;

      o_td.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_td.log_err;
         ROLLBACK;
         RAISE;
   END process_files;
END td_fileapi;
/