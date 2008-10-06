CREATE OR REPLACE PACKAGE BODY trans_files
IS
   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   FUNCTION calc_rej_ind(
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
        FROM file_object_detail
       WHERE file_label = p_file_label
         AND processed_ts =
                         ( SELECT MAX( processed_ts )
                            FROM file_object_detail
                           WHERE file_label = p_file_label );

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
      p_directory   VARCHAR2,
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
                                  p_directory      => p_directory,
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
   PROCEDURE process_file(
      p_file_label   VARCHAR2,
      p_directory    VARCHAR2 DEFAULT NULL
   )
   IS
      l_rows      BOOLEAN    := FALSE;
      o_label     file_label_ot := trans_factory.get_file_label_ot( p_file_label => p_file_label, p_directory => p_directory );
      o_ev        evolve_ot  := evolve_ot( p_module => 'process_file' );
   BEGIN

      -- process the file
      o_label.process;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END process_file;

   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   PROCEDURE process_group(
      p_file_group   VARCHAR2,
      p_label_type   VARCHAR2 DEFAULT NULL
   )
   IS
      l_rows      BOOLEAN    := FALSE;
      o_label     file_label_ot;
      o_ev        evolve_ot  := evolve_ot( p_module => 'process_group' );
   BEGIN
      FOR c_labels IN ( SELECT  file_label, label_type
                            FROM file_conf
                          WHERE lower(file_group) = lower(p_file_group)
                            AND REGEXP_LIKE( label_type, NVL( p_label_type, '.' ), 'i' )
                        ORDER BY label_type DESC )
      LOOP
         -- catch empty cursors
         l_rows := TRUE;
         
         -- use the factory to pull the concrete label_type
         o_label  := trans_factory.get_file_label_ot( p_file_label => c_labels.file_label );
         
         -- now process the file
         o_label.process;

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
   END process_group;

END trans_files;
/

SHOW errors