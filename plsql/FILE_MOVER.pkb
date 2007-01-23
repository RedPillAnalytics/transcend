CREATE OR REPLACE PACKAGE BODY efw.file_mover
IS
-- writes information in the FILE_DTL table about files found in SOURCE_DIR
-- SOURCE_DIR is configured in the FILE_CTL table
   PROCEDURE audit_file (
      p_jobnumber      NUMBER,
      p_filename       VARCHAR2,
      p_archfilename   VARCHAR2,
      p_num_bytes      NUMBER,
      p_file_dt        DATE,
      p_debug          BOOLEAN DEFAULT FALSE)
   AS
      r_file_ctl   file_ctl%ROWTYPE;
      l_app        app_info   := app_info (p_module      => 'FILE_MOVER.AUDIT_FILE',
                                           p_debug       => p_debug);
   BEGIN
      SELECT *
        INTO r_file_ctl
        FROM file_ctl
       WHERE jobnumber = p_jobnumber;

      l_app.set_action ('Insert FILE_DTL');

      -- INSERT into the FILE_DTL table to record the movement
      INSERT INTO file_dtl
                  (filename,
                   archfilename,
                   jobname,
                   num_bytes,
                   file_dt,
                   processed_ts,
                   session_id,
                   jobnumber,
                   ext_tab_ind,
                   alt_ext_tab_ind,
                   filenumber)
           VALUES (p_filename,
                   p_archfilename,
                   r_file_ctl.jobname,
                   p_num_bytes,
                   p_file_dt,
                   CURRENT_TIMESTAMP,
                   SYS_CONTEXT ('USERENV', 'SESSIONID'),
                   p_jobnumber,
                   'N',
                   DECODE (REGEXP_SUBSTR (r_file_ctl.multi_files_action, '^All', 1, 1, 'i'),
                           'ALL', 'Y',
                           'N'),
                   file_dtl_seq.NEXTVAL);

      -- IF the size threshholds are not met, then fail the job
      -- ALL the copies occur successfully, but nothing else happens
      l_app.set_action ('Check file details');

      IF p_num_bytes >= r_file_ctl.max_bytes AND r_file_ctl.max_bytes <> 0
      THEN
         raise_application_error (-20001, 'File size larger than MAX_BYTES');
      ELSIF p_num_bytes < r_file_ctl.min_bytes
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

   --  configure a file to be moved for a particular job
   -- there can be multiple files per job
   PROCEDURE register_file (
      p_jobnumber            NUMBER DEFAULT NULL,
      p_jobname              VARCHAR2 DEFAULT NULL,
      p_filename             VARCHAR2 DEFAULT NULL,
      p_source_regexp        VARCHAR2 DEFAULT NULL,
      p_regexp_ci_ind        VARCHAR2 DEFAULT NULL,
      p_source_dir           VARCHAR2 DEFAULT NULL,
      p_min_bytes            NUMBER DEFAULT NULL,
      p_max_bytes            NUMBER DEFAULT NULL,
      p_arch_dir             VARCHAR2 DEFAULT NULL,
      p_add_arch_ts_ind      VARCHAR2 DEFAULT NULL,
      p_wrk_dir              VARCHAR2 DEFAULT NULL,
      p_ext_dir              VARCHAR2 DEFAULT NULL,
      p_ext_filename         VARCHAR2 DEFAULT NULL,
      p_ext_table            VARCHAR2 DEFAULT NULL,
      p_ext_tab_owner        VARCHAR2 DEFAULT NULL,
      p_multi_files_action   VARCHAR2 DEFAULT NULL,
      p_files_required_ind   VARCHAR2 DEFAULT NULL)
   IS
      r_file_ctl   file_ctl%ROWTYPE;
      l_app        app_info           := app_info (p_module => 'FILE_MOVER.REGISTER_JOB_FILE');
   BEGIN
      SELECT NVL (p_jobnumber, jobnumber),
             NVL (p_jobname, jobname),
             NVL (p_filename, filename),
             NVL (p_source_regexp, source_regexp),
             NVL (p_regexp_ci_ind, regexp_ci_ind),
             NVL (p_source_dir, source_dir),
             NVL (p_min_bytes, min_bytes),
             NVL (p_max_bytes, max_bytes),
             NVL (p_arch_dir, arch_dir),
             NVL (p_add_arch_ts_ind, add_arch_ts_ind),
             NVL (p_wrk_dir, wrk_dir),
             NVL (p_ext_dir, ext_dir),
             NVL (p_ext_filename, ext_filename),
             NVL (p_ext_table, ext_table),
             NVL (p_ext_tab_owner, ext_tab_owner),
             NVL (p_multi_files_action, multi_files_action),
             NVL (p_files_required_ind, files_required_ind),
             created_user,
             created_dt,
             modified_user,
             modified_dt
        INTO r_file_ctl
        FROM file_ctl
       WHERE jobnumber = p_jobnumber OR (filename = p_filename AND jobname = p_jobname);

      IF    r_file_ctl.wrk_dir = r_file_ctl.source_dir
         OR r_file_ctl.arch_dir = r_file_ctl.source_dir
         OR r_file_ctl.ext_dir = r_file_ctl.source_dir
      THEN
         raise_application_error (-20001,
                                  'Target directory cannot be the same as source directory');
      END IF;

      l_app.set_action ('Update FILE_CTL');

      UPDATE file_ctl
         SET jobname = r_file_ctl.jobname,
             filename = r_file_ctl.filename,
             source_regexp = r_file_ctl.source_regexp,
             regexp_ci_ind = r_file_ctl.regexp_ci_ind,
             source_dir = r_file_ctl.source_dir,
             min_bytes = r_file_ctl.min_bytes,
             max_bytes = r_file_ctl.max_bytes,
             arch_dir = r_file_ctl.arch_dir,
             add_arch_ts_ind = r_file_ctl.add_arch_ts_ind,
             wrk_dir = r_file_ctl.wrk_dir,
             ext_dir = r_file_ctl.ext_dir,
             ext_filename = r_file_ctl.ext_filename,
             ext_table = r_file_ctl.ext_table,
             ext_tab_owner = r_file_ctl.ext_tab_owner,
             multi_files_action = r_file_ctl.multi_files_action,
             files_required_ind = r_file_ctl.files_required_ind,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE jobnumber = r_file_ctl.jobnumber;

      l_app.clear_app_info;
   EXCEPTION
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error (-20001, 'Invalid combination of parameters');
      WHEN NO_DATA_FOUND
      THEN
         INSERT INTO file_ctl
                     (jobnumber,
                      jobname,
                      filename,
                      source_regexp,
                      regexp_ci_ind,
                      source_dir,
                      min_bytes,
                      max_bytes,
                      arch_dir,
                      add_arch_ts_ind,
                      wrk_dir,
                      ext_dir,
                      ext_filename,
                      ext_table,
                      ext_tab_owner,
                      multi_files_action,
                      files_required_ind,
                      created_user,
                      created_dt)
              VALUES (file_ctl_seq.NEXTVAL,
                      p_jobname,
                      p_filename,
                      p_source_regexp,
                      p_regexp_ci_ind,
                      p_source_dir,
                      p_min_bytes,
                      p_max_bytes,
                      p_arch_dir,
                      p_add_arch_ts_ind,
                      p_wrk_dir,
                      p_ext_dir,
                      p_ext_filename,
                      p_ext_table,
                      p_ext_tab_owner,
                      p_multi_files_action,
                      p_files_required_ind,
                      SYS_CONTEXT ('USERENV', 'SESSION_USER'),
                      SYSDATE);
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END register_file;

   -- moves all the files associated with a particular job into place
   -- also writes those files to an archive directory and a working directory (if configured)
   PROCEDURE process_ext_table (p_jobnumber VARCHAR2, p_debug BOOLEAN DEFAULT FALSE)
   IS
      l_file_dt      file_dtl.file_dt%TYPE;
      l_is_extfile   BOOLEAN;
      l_rows         BOOLEAN                 DEFAULT FALSE;
      l_file_cnt     NUMBER                  := 1;
      l_app          app_info
                       := app_info (p_module      => 'FILE_MOVER.PROCESS_EXT_TABLE',
                                    p_debug       => p_debug);
   BEGIN
      FOR c_file_ctl IN (SELECT *
                           FROM file_ctl
                          WHERE jobnumber = p_jobnumber)
      LOOP
         -- USE this to catch an empty dynamic cursor
         l_rows := TRUE;

         CASE
            WHEN c_file_ctl.multi_files_action = 'NEWEST'
            THEN
               l_app.set_action ('Process NEWEST files');

               -- this means that we only want to process the newest file
               -- we get this by looking for the max timestamp on the file
               UPDATE file_dtl
                  SET ext_tab_ind = 'Y',
                      ext_filename = c_file_ctl.ext_filename
                WHERE jobnumber = p_jobnumber
                  AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
                  AND ext_tab_ind = 'N'
                  AND file_dt IN (
                         SELECT MAX (file_dt)
                           FROM file_dtl
                          WHERE jobnumber = p_jobnumber
                            AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
                            AND ext_tab_ind = 'N');
            WHEN c_file_ctl.multi_files_action = 'OLDEST'
            THEN
               l_app.set_action ('Process OLDEST files');

               -- IN this example, we want to load the oldest file
               -- this time we use a MIN, not a MAX
               UPDATE file_dtl
                  SET ext_tab_ind = 'Y',
                      ext_filename = c_file_ctl.ext_filename
                WHERE jobnumber = p_jobnumber
                  AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
                  AND ext_tab_ind = 'N'
                  AND file_dt IN (
                         SELECT MIN (file_dt)
                           FROM file_dtl
                          WHERE jobnumber = p_jobnumber
                            AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
                            AND ext_tab_ind = 'N');
            WHEN REGEXP_LIKE (c_file_ctl.multi_files_action, '^(all).+', 'i')
            THEN
               l_app.set_action ('Process ALL files');

               -- IN this example, we want all the files to get loaded
               -- IN order to write all the files to the same location, we append "_ROWNUM" to the end of each on
               -- the external table can then be altered to contain all the files by executing ALTER_EXT_TABLE
               UPDATE file_dtl
                  SET ext_tab_ind = 'Y',
                      ext_filename =
                         CASE ROWNUM
                            WHEN 1
                               THEN c_file_ctl.ext_filename
                            ELSE REGEXP_REPLACE (c_file_ctl.ext_filename,
                                                 '\\.',
                                                 '_' || ROWNUM || '.')
                         END
                WHERE jobnumber = p_jobnumber
                  AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
                  AND ext_tab_ind = 'N';
            WHEN c_file_ctl.multi_files_action = 'ABORT'
            THEN
               raise_application_error
                  (-20001,
                   'Multiple files matched the SOURCE_REGEXP with a MULTI_FILES_ACTION value of "ABORT"');
            ELSE
               -- there are several other scenarious I want to create, but this handles everything else for now.
               raise_application_error
                               (-20001,
                                   'Currently, "'
                                || c_file_ctl.multi_files_action
                                || '" is not a suported value for FILE_CTL column MULTI_FILES_ACTION.');
         END CASE;
      END LOOP;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END process_ext_table;

   -- alters the external table to contain all the files specified in the file_dtl table for the particular external table
   PROCEDURE alter_ext_table (
      p_owner       VARCHAR2,
      p_table       VARCHAR2,
      p_files_req   BOOLEAN DEFAULT TRUE,
      p_debug       BOOLEAN DEFAULT FALSE)
   AS
      l_ddl        VARCHAR2 (32000);
      e_no_files   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_files, -1756);
      l_app        app_info
                         := app_info (p_module      => 'FILE_MOVER.ALTER_EXT_TABLE',
                                      p_debug       => p_debug);
   BEGIN
      SELECT    'alter table '
             || p_owner
             || '.'
             || p_table
             || ' location ('
             || REGEXP_REPLACE (stragg (LOCATION), ',', ''',')
             || ''')'
        INTO l_ddl
        FROM (SELECT DISTINCT utility.get_dir_name (fc.ext_dir) || ':''' || fd.ext_filename
                                                                                           LOCATION
                         FROM file_ctl fc JOIN file_dtl fd USING (jobnumber)
                        WHERE fd.ext_tab_ind = 'Y'
                          AND fc.ext_table = p_table
                          AND fc.ext_tab_owner = p_owner
                          AND fd.alt_ext_tab_ind = 'Y');

      l_app.set_action ('Alter external table');

      IF p_debug
      THEN
         job.log_msg ('The location DDL statement: ' || l_ddl);
      ELSE
         EXECUTE IMMEDIATE l_ddl;

         job.log_msg (p_owner || '.' || p_table || ' altered');
      END IF;

      l_app.set_action ('Update FILE_DTL table');

      IF NOT p_debug
      THEN
         UPDATE (SELECT fd.alt_ext_tab_ind ind_to_update
                   FROM file_ctl fc JOIN file_dtl fd USING (jobnumber)
                  WHERE fd.alt_ext_tab_ind = 'Y'
                    AND fd.ext_tab_ind = 'Y'
                    AND fc.ext_table = p_table
                    AND fc.ext_tab_owner = p_owner)
            SET ind_to_update = 'N';
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN e_no_files
      THEN
         IF p_files_req
         THEN
            raise_application_error (-20001,
                                     'There were no files moved into place for this external table');
         END IF;
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END alter_ext_table;

   -- audits information about external tables after the file(s) have been put in place
   PROCEDURE audit_ext_table (p_jobnumber VARCHAR2, p_debug BOOLEAN DEFAULT FALSE)
   IS
      r_file_ctl    file_ctl%ROWTYPE;
      l_num_rows    NUMBER;
      l_num_lines   NUMBER;
      l_pct_miss    NUMBER;
      l_sql         VARCHAR2 (100);
      e_no_table    EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_table, -942);
      l_app         app_info
                         := app_info (p_module      => 'FILE_MOVER.AUDIT_EXT_TABLE',
                                      p_debug       => p_debug);
   BEGIN
      SELECT *
        INTO r_file_ctl
        FROM file_ctl
       WHERE jobnumber = p_jobnumber;

      IF r_file_ctl.multi_files_action = 'ALL AUTO'
      THEN
         alter_ext_table (r_file_ctl.ext_tab_owner, r_file_ctl.ext_table, TRUE, p_debug);
      END IF;

      l_app.set_action ('Get count from table');
      l_sql := 'SELECT count(*) FROM ' || r_file_ctl.ext_tab_owner || '.' || r_file_ctl.ext_table;

      IF p_debug
      THEN
         job.log_msg ('Count SQL: ' || l_sql);
      ELSE
         EXECUTE IMMEDIATE l_sql
                      INTO l_num_rows;

         BEGIN
            SELECT num_lines,
                   100 - ((l_num_rows / num_lines) * 100) pct_miss
              INTO l_num_lines,
                   l_pct_miss
              FROM (SELECT SUM (num_lines) num_lines
                      FROM file_dtl
                     WHERE ext_tab_ind = 'Y'
                       AND jobnumber = p_jobnumber
                       AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID'));
         EXCEPTION
            WHEN ZERO_DIVIDE
            THEN
               job.log_msg ('External table location is an empty file');
         END;

         INSERT INTO ext_tab_dtl
                     (ext_table,
                      ext_tab_owner,
                      jobname,
                      jobnumber,
                      processed_ts,
                      num_rows,
                      num_lines,
                      reject_pcnt,
                      session_id)
              VALUES (r_file_ctl.ext_table,
                      r_file_ctl.ext_tab_owner,
                      r_file_ctl.jobname,
                      p_jobnumber,
                      CURRENT_TIMESTAMP,
                      CASE r_file_ctl.multi_files_action
                         WHEN 'ALL MANUAL'
                            THEN NULL
                         ELSE l_num_rows
                      END,
                      CASE r_file_ctl.multi_files_action
                         WHEN 'ALL MANUAL'
                            THEN NULL
                         ELSE l_num_lines
                      END,
                      CASE r_file_ctl.multi_files_action
                         WHEN 'ALL MANUAL'
                            THEN NULL
                         ELSE l_pct_miss
                      END,
                      SYS_CONTEXT ('USERENV', 'SESSIONID'));
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN e_no_table
      THEN
         raise_application_error (-20001, 'The external table in EXT_TAB_NAME does not exist.');
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END audit_ext_table;

   -- marks all the files encompassing an external table as processed for a jobnumber
   PROCEDURE complete_ext_table (p_jobnumber NUMBER, p_debug BOOLEAN DEFAULT FALSE)
   IS
      l_app   app_info
                      := app_info (p_module      => 'FILE_MOVER.COMPLETE_EXT_TABLE',
                                   p_debug       => p_debug);
   BEGIN
      UPDATE file_dtl
         SET ext_tab_ind = 'P'
       WHERE jobnumber = p_jobnumber
         AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
         AND ext_tab_ind = 'Y';

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END complete_ext_table;

   -- marks entries in a the log table to an error status
   -- probably never get called as the log table records don't get commited now unless job is successful
   PROCEDURE error_ext_table (p_debug BOOLEAN DEFAULT FALSE)
   IS
      l_app   app_info := app_info (p_module => 'FILE_MOVER.ERROR_EXT_TABLE', p_debug => p_debug);
   BEGIN
      UPDATE file_dtl
         SET ext_tab_ind = 'E'
       WHERE session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
         AND alt_ext_tab_ind = 'Y'
         AND ext_tab_ind = 'N';

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END error_ext_table;

   -- records the number of lines in each external table file
   PROCEDURE record_numlines (p_filenumber NUMBER, p_debug BOOLEAN DEFAULT FALSE)
   IS
      l_ext_dir        file_ctl.ext_dir%TYPE;
      l_ext_filename   file_dtl.ext_filename%TYPE;
      l_numlines       NUMBER;
      l_app            app_info
                         := app_info (p_module      => 'FILE_MOVER.RECORD_NUMLINES',
                                      p_debug       => p_debug);
   BEGIN
      SELECT fc.ext_dir,
             fd.ext_filename
        INTO l_ext_dir,
             l_ext_filename
        FROM file_ctl fc JOIN file_dtl fd ON (fc.jobnumber = fd.jobnumber)
       WHERE filenumber = p_filenumber;

      -- in debug mode, this might render incorrect results as the file might still be zipped
      -- so we don't do this, we signify it, and store a "0" instead
      IF p_debug
      THEN
         job.log_msg (   'UTILITY.GET_NUMLINES: '
                      || utility.get_dir_name (l_ext_dir)
                      || ', '
                      || l_ext_filename);
         job.log_msg ('Returning 0 instead of number of lines because of debug mode');
         l_numlines := 0;
      ELSE
         l_numlines := utility.get_numlines (utility.get_dir_name (l_ext_dir), l_ext_filename);
      END IF;

      UPDATE file_dtl
         SET num_lines = l_numlines
       WHERE filenumber = p_filenumber;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END record_numlines;

   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   FUNCTION calc_rej_ind (p_jobnumber NUMBER, p_rej_limit NUMBER DEFAULT 20)
      RETURN VARCHAR2
   IS
      l_pct_miss   NUMBER;
      l_rej_ind    VARCHAR2 (1);
      l_app        app_info     := app_info (p_module => 'FILE_MOVER.CALC_REJ_IND');
   BEGIN
      SELECT reject_pcnt
        INTO l_pct_miss
        FROM ext_tab_dtl
       WHERE jobnumber = p_jobnumber AND processed_ts = (SELECT MAX (processed_ts)
                                                           FROM ext_tab_dtl
                                                          WHERE jobnumber = p_jobnumber);

      IF l_pct_miss > p_rej_limit
      THEN
         RETURN 'N';
      ELSE
         RETURN 'Y';
      END IF;

      l_app.clear_app_info;
   END calc_rej_ind;

   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   PROCEDURE process_job (
      p_jobname       VARCHAR2,
      p_filename      VARCHAR2 DEFAULT NULL,
      p_keep_source   BOOLEAN DEFAULT FALSE,
      p_debug         BOOLEAN DEFAULT FALSE)
   IS
      l_rows_ctl     BOOLEAN        := FALSE;                             -- TO catch empty cursors
      l_rows_dtl     BOOLEAN;                                             -- TO catch empty cursors
      l_rows_list    BOOLEAN;                                             -- TO catch empty cursors
      l_numlines     NUMBER;
      l_cmd          VARCHAR2 (500);
      l_filename     VARCHAR2 (200);
      l_numfiles     NUMBER;
      l_passphrase   VARCHAR2 (100) := 'dataBase~maRkeTing:comPanY';
      l_app          app_info
         := app_info (p_module           => 'FILE_MOVER.PROCESS_JOB',
                      p_client_info      => NVL (SYS_CONTEXT ('USERENV', 'CLIENT_INFO'), p_jobname),
                      p_debug            => p_debug);
   BEGIN
      -- CREATE a cursor containing the DDL from the target indexes
      FOR c_file_ctl IN (SELECT   REGEXP_REPLACE (source_dir, '//', '/', 1, 1, 'i') source_dir,
                                  REGEXP_REPLACE (arch_dir, '//', '/', 1, 1, 'i') arch_dir,
                                  jobname,
                                  jobnumber,
                                  source_regexp,
                                  regexp_ci_ind,
                                  min_bytes,
                                  max_bytes,
                                  add_arch_ts_ind,
                                  wrk_dir,
                                  ext_dir,
                                  ext_filename,
                                  ext_table,
                                  ext_tab_owner,
                                  multi_files_action,
                                  files_required_ind,
                                  created_user,
                                  created_dt,
                                  modified_user,
                                  modified_dt
                             FROM file_ctl
                            WHERE jobname = p_jobname
                              AND REGEXP_LIKE (filename, DECODE (p_filename, NULL, '?', p_filename))
                         ORDER BY jobnumber)
      LOOP
         l_rows_dtl := FALSE;
         l_rows_list := FALSE;
         job.log_msg ('Jobnumber: ' || c_file_ctl.jobnumber || ', Regexp: '
                      || c_file_ctl.source_regexp);
         -- catch empty cursor sets
         l_rows_ctl := TRUE;
         l_app.set_action ('Evaluate SOURCE_DIR contents');
         -- use java stored procedure to populate global temp table DIR_LIST with all the files in the directory
         util.get_dir_list (c_file_ctl.source_dir);

         -- look at the contents of the DIR_LIST table for
         FOR c_dir_list IN (SELECT filename,
                                   c_file_ctl.source_dir || '/' || filename sourcefile,
                                      c_file_ctl.arch_dir
                                   || '/'
                                   || filename
                                   || DECODE (c_file_ctl.add_arch_ts_ind,
                                              'Y', '.' || TO_CHAR (SYSDATE, 'yyyymmddhhmiss'),
                                              NULL) archfile,
                                   c_file_ctl.wrk_dir || '/' || filename wrkfile,
                                   file_dt,
                                   file_size
                              FROM dir_list
                             WHERE REGEXP_LIKE (filename,
                                                c_file_ctl.source_regexp,
                                                DECODE (c_file_ctl.regexp_ci_ind, 'Y', 'i', NULL)))
         LOOP
            -- catch empty cursor sets
            l_rows_list := TRUE;
            l_app.set_action ('Copy archivefile');
            -- copy file to the archive location
            l_cmd := 'cp -p ' || c_dir_list.sourcefile || ' ' || c_dir_list.archfile;

            IF p_debug
            THEN
               job.log_msg ('Run_cmd: ' || l_cmd);
            ELSE
               util.run_cmd (l_cmd);
               job.log_msg (c_dir_list.sourcefile || ' copied to ' || c_dir_list.archfile);
            END IF;

            l_app.set_action ('Copy wrkfile');

            -- copy file to the work location if it's configured
            -- START using the archfile location as the source
            IF UPPER (c_file_ctl.wrk_dir) <> 'NA' AND c_dir_list.archfile <> c_dir_list.wrkfile
            THEN
               l_cmd := 'cp -p ' || c_dir_list.archfile || ' ' || c_dir_list.wrkfile;

               IF p_debug
               THEN
                  job.log_msg ('Run_cmd: ' || l_cmd);
               ELSE
                  util.run_cmd (l_cmd);
                  job.log_msg (c_dir_list.archfile || ' copied to ' || c_dir_list.wrkfile);
               END IF;
            ELSIF c_dir_list.archfile = c_dir_list.wrkfile
            THEN
               job.log_msg ('Work file is the same as the Arch file');
            ELSE
               job.log_msg ('WRK_DIR is null... no additional files copied');
            END IF;

            -- WRITE an audit record for the file that was just archived
            audit_file (p_jobnumber         => c_file_ctl.jobnumber,
                        p_filename          => c_dir_list.filename,
                        p_archfilename      => c_dir_list.archfile,
                        p_num_bytes         => c_dir_list.file_size,
                        p_file_dt           => c_dir_list.file_dt,
                        p_debug             => p_debug);
         END LOOP;

         l_app.set_action ('Check for matching files');

         CASE
            WHEN NOT l_rows_list AND c_file_ctl.files_required_ind = 'Y'
            THEN
               raise_application_error (-20001, 'No files found... FILES_REQUIRED_IND is "Y"');
            WHEN NOT l_rows_list AND c_file_ctl.files_required_ind = 'N'
            THEN
               job.log_msg ('No files found... but FILES_REQUIRED_IND is "N"');
               l_app.set_action ('Empty previous files');

               -- if FILES_REQUIRED_IND is N, then we need to empty out the current files
               -- a process flow shouldn't proceed with old files there
               FOR c_location IN (SELECT    'cp /dev/null '
                                         || utility.get_dir_path (directory_name)
                                         || '/'
                                         || LOCATION cmd,
                                         utility.get_dir_path (directory_name) || '/' || LOCATION
                                                                                           ext_file
                                    FROM dba_external_locations
                                   WHERE owner = c_file_ctl.ext_tab_owner
                                     AND table_name = c_file_ctl.ext_table)
               LOOP
                  IF p_debug
                  THEN
                     job.log_msg ('Run_cmd: ' || c_location.cmd);
                  ELSE
                     util.run_cmd (c_location.cmd);
                     job.log_msg (c_location.ext_file || ' emptied out');
                  END IF;
               END LOOP;
            WHEN l_rows_list
            THEN
               l_app.set_action ('Process external tables');
               -- make updates to the audit records based on business logic for this job
               -- this will modify the data so that the next cursor is the correct one
               process_ext_table (p_jobnumber => c_file_ctl.jobnumber);

               -- IF the statement above succeeded, then we use the following cursor to process the external table
               -- WORK on the multi_file_action column and do updates where appropriate
               FOR c_file_dtl IN (SELECT    c_file_ctl.ext_dir
                                         || '/'
                                         || REGEXP_REPLACE (filename, '//', '/', 1, 1, 'i')
                                                                                           ext_file,
                                            c_file_ctl.ext_dir
                                         || '/'
                                         || REGEXP_REPLACE (ext_filename, '//', '/', 1, 1, 'i')
                                                                                        ext_file_mv,
                                         archfilename,
                                         ext_filename,
                                         filename,
                                         filenumber
                                    FROM file_dtl
                                   WHERE jobnumber = c_file_ctl.jobnumber
                                     AND ext_tab_ind = 'Y'
                                     AND session_id = SYS_CONTEXT ('USERENV', 'SESSIONID'))
               LOOP
                  -- catch empty cursors
                  l_rows_dtl := TRUE;

                  IF p_debug
                  THEN
                     job.log_msg ('The filenumber is: ' || c_file_dtl.filenumber);
                  END IF;

                  -- WRITE to the external table directory if defined
                  l_app.set_action ('Copy external table files');

                  IF UPPER (c_file_ctl.ext_filename) <> 'NA' AND c_file_dtl.ext_filename IS NOT NULL
                  THEN
                     l_cmd := 'cp -p ' || c_file_dtl.archfilename || ' ' || c_file_dtl.ext_file;

                     IF p_debug
                     THEN
                        job.log_msg ('Run_cmd: ' || l_cmd);
                     ELSE
                        util.run_cmd (l_cmd);
                        job.log_msg (c_file_dtl.archfilename || ' copied to ' || c_file_dtl.ext_file);
                     END IF;
                  END IF;

                  -- decrypt the file if it's encrypted
                  -- currently only supports gpg
                  -- decrypt_file will return the decrypted filename
                  -- IF the file isn't a recognized encrypted file type, it just returns the name passed
                  l_filename :=
                     utility.decrypt_file (c_file_ctl.ext_dir,
                                           c_file_dtl.filename,
                                           l_passphrase,
                                           p_debug);
                  -- unzip the file if it's zipped
                  -- currently will unzip, or gunzip, or bunzip2 or uncompress
                  -- unzip_file will return the unzipped filename
                  -- IF the file isn't a recognized zip archive file, it just returns the name passed
                  l_filename :=
                               utility.unzip_file (c_file_ctl.ext_dir, c_file_dtl.filename, p_debug);
                  -- now move the file to the expected name
                  l_cmd := 'mv ' || l_filename || ' ' || c_file_dtl.ext_file_mv;

                  IF p_debug
                  THEN
                     job.log_msg ('Run_cmd: ' || l_cmd);
                  ELSE
                     util.run_cmd (l_cmd);
                     job.log_msg (l_filename || ' moved to ' || c_file_dtl.ext_file_mv);
                  END IF;

                  -- get the number of lines and record it back to the table with rowid
                  record_numlines (c_file_dtl.filenumber, p_debug);
               END LOOP;

               l_app.set_action ('Delete source files');

               -- IF we get this far, then we need to delete the source files
               -- this step is ignored if p_keep_source = TRUE
               IF NOT p_keep_source
               THEN
                  FOR c_dir_list IN (SELECT filename,
                                            c_file_ctl.source_dir || '/' || filename sourcefile
                                       FROM dir_list
                                      WHERE REGEXP_LIKE (filename,
                                                         c_file_ctl.source_regexp,
                                                         DECODE (c_file_ctl.regexp_ci_ind,
                                                                 'Y', 'i',
                                                                 NULL)))
                  LOOP
                     l_cmd := 'rm ' || c_dir_list.sourcefile;

                     IF p_debug
                     THEN
                        job.log_msg ('Run_cmd: ' || l_cmd);
                     ELSE
                        util.run_cmd (l_cmd);
                        job.log_msg (c_dir_list.sourcefile || ' deleted');
                     END IF;
                  END LOOP;
               END IF;

               l_app.set_action ('Audit external tables');
               -- WRITE audit information about the external table
               audit_ext_table (c_file_ctl.jobnumber, p_debug);
               -- need to close all the files in the FILE_DTL table as processed
               -- this was not necessary when using file_mover.pl because each file was processed with a new session
               -- this really was a bug, as the possiblity of executing separate File Mover jobs in the same session was always possible in theory
               complete_ext_table (c_file_ctl.jobnumber);
         END CASE;

         IF p_debug
         THEN
            ROLLBACK;
         ELSE
            COMMIT;
         END IF;
      END LOOP;

      IF NOT l_rows_ctl
      THEN
         raise_application_error (-20001, 'No File Mover jobs defined for this jobname');
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         error_ext_table;
         RAISE;
   END process_job;

   -- created for backward compatibility
   -- the file_mover.pl was jobnumber based
   PROCEDURE process_jobnumber (
      p_jobnumber     NUMBER,
      p_keep_source   BOOLEAN DEFAULT FALSE,
      p_debug         BOOLEAN DEFAULT FALSE)
   AS
      l_jobname    file_ctl.jobname%TYPE;
      l_filename   file_ctl.filename%TYPE;
      l_app        app_info
                       := app_info (p_module      => 'FILE_MOVER.PROCESS_JOBNUMBER',
                                    p_debug       => p_debug);
   BEGIN
      SELECT jobname,
             filename
        INTO l_jobname,
             l_filename
        FROM file_ctl
       WHERE jobnumber = p_jobnumber;

      process_job (l_jobname, l_filename, p_keep_source, p_debug);
      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         error_ext_table;
         RAISE;
   END process_jobnumber;
END file_mover;
/