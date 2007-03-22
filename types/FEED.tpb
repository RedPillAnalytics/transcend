CREATE OR REPLACE TYPE BODY tdinc.feed
AS
   -- calculates whether the anticipated number of rejected (bad) records meets a certain threshhold, which is specified in terms of percentage
   MEMBER FUNCTION calc_rej_ind (p_rej_limit NUMBER DEFAULT 20)
      RETURN VARCHAR2
   IS
      l_pct_miss   NUMBER;
      l_rej_ind    VARCHAR2 (1);
      o_app        applog       := applog (p_module => 'FILE_MOVER.CALC_REJ_IND');
   BEGIN
      SELECT reject_pcnt
        INTO l_pct_miss
        FROM filehub_obj_detail
       WHERE filehub_id = SELF.filehub_id AND processed_ts = (SELECT MAX (processed_ts)
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
   -- processes files for a particular job
   -- if P_FILENAME is null, then all files are processed
   MEMBER PROCEDURE process_feed (p_keep_source BOOLEAN DEFAULT FALSE)
   IS
      l_rows         BOOLEAN        := FALSE;                             -- TO catch empty cursors
      l_numlines     NUMBER;
      l_cmd          VARCHAR2 (500);
      l_filename     VARCHAR2 (200);
      l_numfiles     NUMBER;
      l_passphrase   VARCHAR2 (100) := 'dataBase~maRkeTing:comPanY';
      o_app          applog := applog (p_module      => 'FEED.PROCESS_FEED',
                                       p_debug       => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Evaluate SOURCE_DIRECTORY contents');
      -- use java stored procedure to populate global temp table DIR_LIST with all the files in the directory
      util.get_dir_list (source_directory);

      -- look at the contents of the DIR_LIST table for
      FOR c_dir_list IN (SELECT filename,
                                CASE ROWNUM
                                   WHEN 1
                                      THEN source_filepath
                                   ELSE REGEXP_REPLACE (source_filepath, '\.', '_' || ROWNUM || '.')
                                END source_filepath,
                                file_dt,
                                file_size,
                                CASE
                                   WHEN SELF.multi_file_action = 'min' AND file_dt = min_file_dt
                                      THEN 'Y'
                                   WHEN SELF.multi_file_action = 'max' AND file_dt = max_file_dt
                                      THEN 'Y'
                                   WHEN SELF.multi_file_action = 'all'
                                      THEN 'Y'
                                   ELSE 'N'
                                END ext_ind
                           FROM (SELECT filename,
                                        source_directory || '/' || filename source_filepath,
                                        file_dt,
                                        file_size,
                                        MAX (file_dt) OVER (PARTITION BY 1) max_file_dt,
                                        MIN (file_dt) OVER (PARTITION BY 1) min_file_dt
                                   FROM dir_list
                                  WHERE REGEXP_LIKE (filename, source_regexp, regexp_options)))
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         o_app.set_action ('Copy archivefile');
         -- copy file to the archive location
         coreutils.copy_file (c_dir_list.source_filepath, arch_filepath, SELF.DEBUG_MODE);
         -- POSSIBLY audit the file later
         -- that way, I'll have numlines handy
              -- WRITE an audit record for the file that was just archived
         audit_file (p_src_filename       => c_dir_list.source_filepath,
                     p_arch_filename      => arch_filepath,
                     p_num_bytes          => c_dir_list.file_size,
                     p_num_lines          => NULL,
                     p_file_dt            => c_dir_list.file_dt);
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
            o_app.set_action ('Empty previous files');

      -- need a java stored procedure to "touch" the file... or "cp /dev/null" to it
      -- this can be done by a simple File object... delete the file and then create it.      

         WHEN l_rows_list
         THEN
            o_app.set_action ('Process external tables');
            -- copy the file to the external table
            o_app.set_action ('Copy external table files');
            coreutils.copy_file (arch_filepath, filepath, SELF.DEBUG_MODE);
            -- decrypt the file if it's encrypted
            -- currently only supports gpg
            -- decrypt_file will return the decrypted filename
            -- IF the file isn't a recognized encrypted file type, it just returns the name passed
            l_filename :=
               utility.decrypt_file (directory, filename, l_passphrase,
                                     self.debug_mode);
            -- unzip the file if it's zipped
            -- currently will unzip, or gunzip, or bunzip2 or uncompress
            -- unzip_file will return the unzipped filename
            -- IF the file isn't a recognized zip archive file, it just returns the name passed
            l_filename := utility.unzip_file (directory, filename, self.debug_mode);
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
            o_app.set_action ('Delete source files');

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

            o_app.set_action ('Audit external tables');
            -- WRITE audit information about the external table
            audit_ext_table (c_file_ctl.jobnumber, p_debug);
            -- need to close all the files in the FILE_DTL table as processed
            -- this was not necessary when using file_mover.pl because each file was processed with a new session
            -- this really was a bug, as the possiblity of executing separate File Mover jobs in the same session was always possible in theory
            complete_ext_table (c_file_ctl.jobnumber);
      END CASE;

      IF NOT l_rows_ctl
      THEN
         raise_application_error (-20001, 'No File Mover jobs defined for this jobname');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         error_ext_table;
         RAISE;
   END process_feed;
END;
/