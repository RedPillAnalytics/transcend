CREATE OR REPLACE PACKAGE BODY tdinc.filehub
IS
   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   FUNCTION calc_rej_ind (
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2,
      p_rej_limit       NUMBER DEFAULT 20)
      RETURN VARCHAR2
   IS
      l_pct_diff   NUMBER;
      l_rej_ind    VARCHAR2 (1);
      o_app        applog       := applog (p_module => 'FILEHUB.CALC_REJ_IND');
   BEGIN
      SELECT percent_diff
        INTO l_pct_diff
        FROM filehub_obj_detail
       WHERE filehub_group = p_filehub_group
         AND filehub_name = p_filehub_name
         AND processed_ts =
                          (SELECT MAX (processed_ts)
                             FROM filehub_obj_detail
                            WHERE filehub_group = p_filehub_group AND filehub_name = p_filehub_name);

      IF l_pct_diff > p_rej_limit
      THEN
         RETURN 'N';
      ELSE
         RETURN 'Y';
      END IF;

      o_app.clear_app_info;
   END calc_rej_ind;

   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   PROCEDURE process (
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2 DEFAULT NULL,
      p_keep_source     varchar2 DEFAULT 'no',
      p_runmode         varchar2 DEFAULT null)
   IS
      l_rows           BOOLEAN                          := FALSE;         -- TO catch empty cursors
      l_filehub_type   filehub_conf.filehub_type%TYPE;
      o_extract        EXTRACT;
      o_feed           feed;
      o_app            applog     := applog (p_module      => 'filehub.process',
                                             p_runmode     => p_runmode);
   BEGIN
      FOR c_fh_conf IN (SELECT   filehub_id,
                                 filehub_type
                            FROM filehub_conf
                           WHERE filehub_group = p_filehub_group
                             AND REGEXP_LIKE (filehub_name,
                                              DECODE (p_filehub_name, NULL, '?', p_filehub_name))
                        ORDER BY filehub_id)
      LOOP
         CASE lower(c_fh_conf.filehub_type)
            WHEN 'extract'
            THEN
               SELECT VALUE (t)
                 INTO o_extract
                 FROM extract_ot t
                WHERE t.filehub_id = c_fh_conf.filehub_id;

         o_extract.runmode := p_runmode;
               o_extract.process;
            WHEN 'feed'
            THEN
               SELECT VALUE (t)
                 INTO o_feed
                 FROM feed_ot t
                WHERE t.filehub_id = c_fh_conf.filehub_id;

         o_feed.runmode := p_runmode;
               o_feed.process (p_keep_source);
            ELSE
               NULL;
         END CASE;

         -- need this commit to clear out the contents of the DIR_LIST table
         COMMIT;
      END LOOP;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         ROLLBACK;
         RAISE;
   END process;
END filehub;
/