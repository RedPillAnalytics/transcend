CREATE OR REPLACE TYPE BODY tdinc.feed
AS
   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   MEMBER FUNCTION calc_rej_ind (p_rej_limit NUMBER DEFAULT 20)
      RETURN VARCHAR2
   IS
      l_pct_miss   NUMBER;
      l_rej_ind    VARCHAR2 (1);
      o_app        applog       := applog (p_module => 'feed.calc_rej_ind');
   BEGIN
      SELECT reject_pcnt
        INTO l_pct_miss
        FROM filehub_obj_detail
       WHERE filehub_id = SELF.filehub_id
         AND processed_ts = (SELECT MAX (processed_ts)
                               FROM filehub_obj_detail
                              WHERE filehub_id = SELF.filehub_id);

      IF l_pct_miss > p_rej_limit
      THEN
         RETURN 'N';
      ELSE
         RETURN 'Y';
      END IF;

      o_app.clear_app_info;
   END calc_rej_ind;
   -- process a feed configured in FILEHUB_CONF
   MEMBER PROCEDURE process_feed (p_keep_source BOOLEAN DEFAULT FALSE)
   IS
      l_rows       BOOLEAN        := FALSE;         -- TO catch empty cursors
      l_numlines   NUMBER;
      l_cmd        VARCHAR2 (500);
      l_filepath   VARCHAR2 (200);
      l_numfiles   NUMBER;
      o_app        applog
         := applog (p_module => 'feed.process_feed',
                    p_debug => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Evaluate SOURCE_DIRECTORY contents');
      -- use java stored procedure to populate global temp table DIR_LIST with all the files in the directory
      util.get_dir_list (source_directory);

      -- look at the contents of the DIR_LIST table for
      FOR c_dir_list IN
         (SELECT source_filename,
                 CASE ROWNUM
                    WHEN 1
                       THEN source_filepath
                    ELSE REGEXP_REPLACE (source_filepath,
                                         '\.',
                                         '_' || ROWNUM || '.')
                 END source_filepath,
                 CASE ROWNUM
                    WHEN 1
                       THEN source_filepath
                    ELSE REGEXP_REPLACE
                           (   coreutils.get_dir_path
                                               (DIRECTORY)
                            || '/'
                            || source_filename,
                            '\.', '_' || ROWNUM || '.')
                 END pre_source_filepath,
                 file_dt, file_size,
                 CASE
                    WHEN LOWER (SELF.multi_file_action) = 'min'
                    AND file_dt = min_file_dt
                       THEN 'Y'
                    WHEN LOWER (SELF.multi_file_action) = 'max'
                    AND file_dt = max_file_dt
                       THEN 'Y'
                    WHEN LOWER (SELF.multi_file_action) = 'all'
                       THEN 'Y'
                    ELSE 'N'
                 END ext_tab_ind
            FROM (SELECT filename source_filename,
                            SELF.source_directory
                         || '/'
                         || filename source_filepath,
                         file_dt, file_size,
                         MAX (file_dt) OVER (PARTITION BY 1) max_file_dt,
                         MIN (file_dt) OVER (PARTITION BY 1) min_file_dt
                    FROM dir_list
                   WHERE REGEXP_LIKE (filename, SELF.source_regexp,
                                      SELF.regexp_options)))
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         -- reset variables used in the cursor
         l_numlines := NULL;
         -- copy file to the archive location
         o_app.set_action ('Copy archivefile');
         coreutils.copy_file (c_dir_list.source_filepath, arch_filepath,
                              SELF.DEBUG_MODE);
         o_app.set_action ('Process external tables');
         -- copy the file to the external table
         o_app.set_action ('Copy external table files');

         IF c_dir_list.ext_tab_ind = 'Y'
         THEN
            -- first move the file to the target destination without changing the name
            -- because the file might be zipped or encrypted
            coreutils.copy_file (arch_filepath, pre_source_filepath,
                                 SELF.DEBUG_MODE);
            -- decrypt the file if it's encrypted
            -- currently only supports gpg
            -- decrypt_file will return the decrypted filename
            -- IF the file isn't a recognized encrypted file type, it just returns the name passed
            l_filepath :=
               coreutils.decrypt_file (DIRECTORY, c_dir_list.source_filename,
                                       SELF.passphrase, SELF.DEBUG_MODE);
            -- unzip the file if it's zipped
            -- currently will unzip, or gunzip, or bunzip2 or uncompress
            -- unzip_file will return the unzipped filename
            -- IF the file isn't a recognized zip archive file, it just returns the name passed
            l_filepath :=
               coreutils.unzip_file (DIRECTORY, c_dir_list.source_filename,
                                     SELF.DEBUG_MODE);
                 -- now move the file to the expected name
            -- do this with a copy/delete
            coreutils.copy_file (l_filepath, filepath, SELF.DEBUG_MODE);
            coreutils.delete_file (c_dir_list.pre_source_filepath);
            -- get the number of lines in the file now that it is decrypted and uncompressed
            l_numlines :=
                       coreutils.get_numlines (SELF.DIRECTORY, SELF.filename);
            o_app.log_msg (l_filepath || ' moved to ' || SELF.source_filepath);
         END IF;

         -- WRITE an audit record for the file that was just archived
         o_app.set_action ('Audit external tables');
         audit_file (p_src_filename => c_dir_list.source_filepath,
                     p_num_bytes => c_dir_list.file_size,
                     p_num_lines => l_numlines,
                     p_file_dt => c_dir_list.file_dt);
         -- WRITE audit information about the external table
         audit_ext_table;
         -- IF we get this far, then we need to delete the source files
         -- this step is ignored if p_keep_source = TRUE
         o_app.set_action ('Delete source files');

         IF NOT p_keep_source
         THEN
            coreutils.delete_file (c_dir_list.source_filepath);
         END IF;
      END LOOP;

      o_app.set_action ('Check for matching files');

      CASE
         WHEN NOT l_rows AND file_required = 'Y'
         THEN
            raise_application_error (o_app.get_err_cd ('no_files_found'),
                                     o_app.get_err_msg ('no_files_found'));
         WHEN NOT l_rows_list AND file_required = 'N'
         THEN
            job.log_msg ('No files found... but FILE_REQUIRED is "N"');
            -- empty out the contents of the file
            o_app.set_action ('Empty previous files');
            coreutils.delete_file (SELF.filepath);
            coreutils.create_file (SELF.filepath);
      END CASE;

      IF NOT l_rows_ctl
      THEN
         raise_application_error (o_app.get_err_cd ('feed_not_configured'),
                                  o_app.get_err_msg ('feed_not_configured'));
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         error_ext_table;
         RAISE;
   END process_feed;
   -- audits information about external tables after the file(s) have been put in place
   MEMBER PROCEDURE audit_object
   IS
      l_num_rows    NUMBER;
      l_num_lines   NUMBER;
      l_pct_miss    NUMBER;
      l_sql         VARCHAR2 (100);
      e_no_table    EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_table, -942);
      l_app         applog
         := applog (p_module => 'feed.audit_object',
                    p_debug => SELF.DEBUG_MODE);
   BEGIN
      IF lower (multi_file_action) = 'all'
      THEN
         alter_ext_table (object_owner, object_table, TRUE);
      END IF;

      l_app.set_action ('Get count from table');
      l_sql :=
            'SELECT count(*) FROM '
         || r_file_ctl.ext_tab_owner
         || '.'
         || r_file_ctl.ext_table;
      ddl_exec (l_sql, SELF.DEBUG_MODE);

      IF NOT SELF.DEBUG_MODE
      THEN
         BEGIN
            SELECT num_lines, 100 - ((l_num_rows / num_lines) * 100) pct_miss
              INTO l_num_lines, l_pct_miss
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

         INSERT INTO filehub_obj_detail
                     (fh_obj_id, filehub_id,
                      filehub_type, filehub_group,
                      object_name, object_owner,
                      num_rows,
                      num_lines,
                      percent_diff
                     )
              VALUES (filehub_obj_detail_seq.NEXTVAL, SELF.filehub_id,
                      SELF.filehub_type, SELF.filehub_group,
                      SELF.object_name, SELF.object_owner,
                      CASE lower (SELF.multi_file_action)
                         WHEN 'all'
                            THEN NULL
                         ELSE l_num_rows
                      END,
                      CASE lower (SELF.multi_file_action)
                         WHEN 'all'
                            THEN NULL
                         ELSE l_num_lines
                      END,
                      CASE lower (SELF.multi_file_action)
                         WHEN 'all'
                            THEN NULL
                         ELSE l_pct_miss
                      END
                     );
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN e_no_table
      THEN
         raise_application_error (o_app.get_err_cd ('no_ext_table'),
                                  o_app.get_err_msg ('no_ext_table'));
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END audit_object;
END;
/