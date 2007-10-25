CREATE OR REPLACE PACKAGE BODY td_fileapi
IS
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

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   FUNCTION extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   IS
      l_cnt           NUMBER           := 0;
      l_head_sql      VARCHAR( 1000 );
      l_extract_sql   VARCHAR2( 1000 );
      o_td            tdtype           := tdtype( p_module => 'extract_object' );
   BEGIN
      -- check that the source object exists and is something we can select from
      td_sql.check_object( p_owner            => p_owner,
                           p_object           => p_object,
                           p_object_type      => 'table$|view'
                         );
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
      td_inst.log_msg( 'Headers query: ' || l_head_sql, 3 );
      td_inst.log_msg( 'Extract query: ' || l_extract_sql, 3 );

      IF NOT td_inst.is_debugmode
      THEN
         IF td_ext.is_true( p_headers )
         THEN
            o_td.change_action( 'Extract headers to file' );
            l_cnt :=
               td_dbapi.extract_query( p_query          => l_head_sql,
                                       p_dirname        => p_dirname,
                                       p_filename       => p_filename,
                                       p_delimiter      => p_delimiter,
                                       p_quotechar      => NULL,
                                       p_append         => p_append
                                     );
         END IF;

         o_td.change_action( 'Extract data to file' );
         l_cnt :=
              l_cnt
            + td_dbapi.extract_query
                                   ( p_query          => l_extract_sql,
                                     p_dirname        => p_dirname,
                                     p_filename       => p_filename,
                                     p_delimiter      => p_delimiter,
                                     p_quotechar      => p_quotechar,
                                     p_append         => CASE
                                        WHEN td_ext.is_true( p_headers )
                                           THEN 'yes'
                                        ELSE p_append
                                     END
                                   );
      END IF;

      o_td.clear_app_info;
      RETURN l_cnt;
   END extract_object;

   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   PROCEDURE process_files(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2 DEFAULT NULL,
      p_keep_source     VARCHAR2 DEFAULT 'no'
   )
   IS
      l_rows      BOOLEAN     := FALSE;                         -- TO catch empty cursors
      o_extract   extracttype;
      o_feed      feedtype;
      o_td        tdtype      := tdtype( p_module => 'process_file' );
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

               o_extract.process;
            WHEN 'feed'
            THEN
               SELECT VALUE( t )
                 INTO o_feed
                 FROM feed_ot t
                WHERE t.filehub_id = c_fh_conf.filehub_id;

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
         raise_application_error( td_inst.get_err_cd( 'incorrect_parameters' ),
                                  td_inst.get_err_msg( 'incorrect_parameters' )
                                );
      END IF;

      o_td.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         ROLLBACK;
         RAISE;
   END process_files;
END td_fileapi;
/