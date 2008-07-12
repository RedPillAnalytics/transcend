CREATE OR REPLACE PACKAGE BODY trans_files
IS
   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   FUNCTION calc_rej_ind(
      p_file_group   VARCHAR2,
      p_file_label   VARCHAR2,
      p_rej_limit    NUMBER DEFAULT 20
   )
      RETURN VARCHAR2
   IS
      l_pct_diff   NUMBER;
      l_rej_ind    VARCHAR2( 1 );
      o_ev         evolve_ot     := evolve_ot( p_module => 'calc_rej_ind' );
   BEGIN
      SELECT percent_diff
        INTO l_pct_diff
        FROM files_obj_detail
       WHERE file_group = p_file_group
         AND file_label = p_file_label
         AND processed_ts =
                         ( SELECT MAX( processed_ts )
                            FROM files_obj_detail
                           WHERE file_group = p_file_group AND file_label = p_file_label );

      IF l_pct_diff > p_rej_limit
      THEN
         RETURN 'N';
      ELSE
         RETURN 'Y';
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END calc_rej_ind;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   PROCEDURE extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
   IS
      l_cnt   NUMBER;
      o_ev    evolve_ot := evolve_ot( p_module => 'extract_object' );
   BEGIN
      l_cnt :=
         td_utils.extract_object( p_owner          => p_owner,
                                 p_object         => p_object,
                                 p_dirname        => p_dirname,
                                 p_filename       => p_filename,
                                 p_delimiter      => p_delimiter,
                                 p_quotechar      => p_quotechar,
                                 p_headers        => p_headers,
                                 p_append         => p_append
                               );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END extract_object;

   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   PROCEDURE process_files( p_file_group VARCHAR2, p_file_label VARCHAR2 DEFAULT NULL )
   IS
      l_rows      BOOLEAN    := FALSE;
      o_extract   extract_ot;
      o_feed      feed_ot;
      o_ev        evolve_ot  := evolve_ot( p_module => 'process_files' );
   BEGIN
      FOR c_fh_conf IN ( SELECT  file_label, file_type
                            FROM files_conf
                          WHERE lower(file_group) = lower(p_file_group)
                             AND REGEXP_LIKE( file_label,
                                              DECODE( p_file_label,
                                                      NULL, '?',
                                                      p_file_label
                                                    )
                                            )
                        ORDER BY file_type DESC )
      LOOP
         l_rows := TRUE;

         CASE LOWER( c_fh_conf.file_type )
            WHEN 'extract'
            THEN
	       o_extract := extract_ot( p_file_group	=> p_file_group,
	    				p_file_label	=> c_fh_conf.file_label );

               o_extract.process;
            WHEN 'feed'
            THEN
 	       o_feed := feed_ot( p_file_group	=> p_file_group,
	    			  p_file_label	=> c_fh_conf.file_label );

               o_feed.process;
            ELSE
               NULL;
         END CASE;

         -- need this commit to clear out the contents of the DIR_LIST table
         COMMIT;
      END LOOP;

      -- no matching files entries are found
      IF NOT l_rows
      THEN
         o_ev.clear_app_info;
	 evolve.raise_err( 'incorrect_parameters' );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         ROLLBACK;
         o_ev.clear_app_info;
         RAISE;
   END process_files;
END trans_files;
/

SHOW errors