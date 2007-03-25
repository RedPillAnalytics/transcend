CREATE OR REPLACE TYPE BODY tdinc.feed
AS
   -- audits information about external tables after the file(s) have been put in place
   MEMBER PROCEDURE audit_ext_tab (p_num_lines NUMBER)
   IS
      l_num_rows   NUMBER         := 0;
      l_pct_miss   NUMBER;
      l_sql        VARCHAR2 (100);
      e_no_table   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_table, -942);
      e_no_files   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_files, -1756);
      o_app        applog  := applog (p_module      => 'feed.audit_ext_tab',
                                      p_debug       => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Get count from table');
      l_sql := 'SELECT count(*) FROM ' || SELF.object_owner || '.' || SELF.object_name;

      IF SELF.DEBUG_MODE
      THEN
         o_app.log_msg ('Count SQL: ' || l_sql);
      ELSE
         EXECUTE IMMEDIATE l_sql
                      INTO l_num_rows;

         BEGIN
            -- calculate the percentage difference
            l_pct_miss := 100 - ((l_num_rows / p_num_lines) * 100);
         EXCEPTION
            WHEN ZERO_DIVIDE
            THEN
               o_app.log_msg ('External table location is an empty file');
         END;

         INSERT INTO filehub_obj_detail
                     (fh_obj_id,
                      filehub_id,
                      filehub_type,
                      filehub_name,
                      filehub_group,
                      object_owner,
                      object_name,
                      num_rows,
                      num_lines,
                      percent_diff)
              VALUES (filehub_obj_detail_seq.NEXTVAL,
                      SELF.filehub_id,
                      SELF.filehub_type,
                      SELF.filehub_name,
                      SELF.filehub_group,
                      SELF.object_owner,
                      SELF.object_name,
                      l_num_rows,
                      p_num_lines,
                      l_pct_miss);
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN e_no_table
      THEN
         raise_application_error (o_app.get_err_cd ('no_ext_tab'),
                                  o_app.get_err_msg ('no_ext_tab'));
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END audit_ext_tab;
   MEMBER PROCEDURE process_feed (p_keep_source BOOLEAN DEFAULT FALSE)
   IS
      l_rows           BOOLEAN         := FALSE;                          -- TO catch empty cursors
      l_numlines       NUMBER;
      l_cmd            VARCHAR2 (500);
      l_filepath       VARCHAR2 (200);
      l_numfiles       NUMBER;
      l_sum_numlines   NUMBER          := 0;
      l_ext_file_cnt   NUMBER;
      l_ext_tab_ddl    VARCHAR2 (2000);
      l_files_url      VARCHAR2 (1000);
      e_no_files       EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_files, -1756);
      o_app            applog
                            := applog (p_module      => 'feed.process_feed',
                                       p_debug       => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Evaluate SOURCE_DIRECTORY contents');
      -- use java stored procedure to populate global temp table DIR_LIST with all the files in the directory
      coreutils.get_dir_list (source_dirpath);

      -- look at the contents of the DIR_LIST table
      -- this first cursor is just to move all matching files to the archive directory
      FOR c_dir_list IN (SELECT filename source_filename,
                                SELF.filepath,
                                SELF.source_dirpath || '/' || filename source_filepath,
                                CASE file_datestamp
                                   WHEN NULL
                                      THEN SELF.arch_dirpath || '/' || filename
                                   ELSE    SELF.arch_dirpath
                                        || '/'
                                        || filename
                                        || '.'
                                        || TO_CHAR (SYSDATE, file_datestamp)
                                END arch_filepath,
                                file_dt,
                                file_size
                           FROM dir_list
                          WHERE REGEXP_LIKE (filename, SELF.source_regexp, SELF.regexp_options))
      LOOP
         -- copy file to the archive location
         o_app.set_action ('Copy archivefile');
         coreutils.copy_file (c_dir_list.source_filepath, c_dir_list.arch_filepath,
                              SELF.DEBUG_MODE);
         o_app.log_msg ('Archive file ' || c_dir_list.arch_filepath || ' created');
      END LOOP;

      -- look at the contents of the DIR_LIST table for
      FOR c_dir_list IN
         (SELECT   source_filename,
                   source_filepath,
                   CASE
                      WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                         THEN REGEXP_REPLACE (filepath, '\.', '_' || file_number || '.')
                      WHEN ext_tab_ind = 'N'
                         THEN NULL
                      ELSE filepath
                   END filepath,
                   CASE
                      WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                         THEN REGEXP_REPLACE (SELF.filename, '\.', '_' || file_number || '.')
                      WHEN ext_tab_ind = 'N'
                         THEN NULL
                      ELSE SELF.filename
                   END filename,
                   pre_mv_filepath,
                   arch_filepath,
                   file_dt,
                   file_size,
                   ext_tab_ind,
                   ext_tab_type_cnt,
                      'alter table '
                   || SELF.object_owner
                   || '.'
                   || SELF.object_name
                   || ' location ('
                   || REGEXP_REPLACE
                         (stragg (   SELF.DIRECTORY
                               || ':'''
                               || CASE
                                     WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                                        THEN REGEXP_REPLACE (SELF.filename,
                                                             '\.',
                                                             '_' || file_number || '.')
                                     WHEN ext_tab_ind = 'N'
                                        THEN NULL
                                     ELSE SELF.filename
                                  END) OVER (PARTITION BY ext_tab_ind),
                          ',',
                          ''',')
                   || ''')' alt_ddl,
                   REGEXP_REPLACE
                      (stragg (   SELF.baseurl
                            || '/'
                            || CASE
                                  WHEN ext_tab_ind = 'Y' AND ext_tab_type_cnt > 1
                                     THEN REGEXP_REPLACE (SELF.filename,
                                                          '\.',
                                                          '_' || file_number || '.')
                                  WHEN ext_tab_ind = 'N'
                                     THEN NULL
                                  ELSE SELF.filename
                               END) OVER (PARTITION BY ext_tab_ind),
                       ',',
                       CHR (10)) files_url
              FROM (SELECT source_filename,
                           source_filepath,
                           filepath,
                           pre_mv_filepath,
                           arch_filepath,
                           file_dt,
                           file_size,
                           ext_tab_ind,
                           RANK () OVER (PARTITION BY 1 ORDER BY ext_tab_ind DESC,
                            source_filename) file_number,
                           COUNT (*) OVER (PARTITION BY ext_tab_ind) ext_tab_type_cnt
                      FROM (SELECT filename source_filename,
                                   baseurl,
                                   SELF.filepath,
                                   SELF.source_dirpath || '/' || filename source_filepath,
                                   SELF.dirpath || '/' || filename pre_mv_filepath,
                                   CASE file_datestamp
                                      WHEN NULL
                                         THEN SELF.arch_dirpath || '/' || filename
                                      ELSE    SELF.arch_dirpath
                                           || '/'
                                           || filename
                                           || '.'
                                           || TO_CHAR (SYSDATE, file_datestamp)
                                   END arch_filepath,
                                   file_dt,
                                   file_size,
                                   CASE
                                      WHEN LOWER (SELF.source_policy) = 'newest'
                                      AND file_dt = MIN (file_dt) OVER (PARTITION BY 1)
                                         THEN 'Y'
                                      WHEN LOWER (SELF.source_policy) = 'oldest'
                                      AND file_dt = MAX (file_dt) OVER (PARTITION BY 1)
                                         THEN 'Y'
                                      WHEN LOWER (SELF.source_policy) = 'all'
                                         THEN 'Y'
                                      ELSE 'N'
                                   END ext_tab_ind
                              FROM dir_list
                             WHERE REGEXP_LIKE (filename, SELF.source_regexp, SELF.regexp_options)))
          ORDER BY ext_tab_ind ASC)
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         -- reset variables used in the cursor
         l_numlines := NULL;
         -- copy the file to the external table
         o_app.set_action ('Copy external table files');

         IF c_dir_list.ext_tab_ind = 'Y'
         THEN
            -- get the DDL to alter the external table after the loop is complete
            -- this statement will be the same no matter which of the rows we pull it from.
            -- might as well use the last
            l_ext_tab_ddl := c_dir_list.alt_ddl;
            -- record the number of external table files
            l_ext_file_cnt := c_dir_list.ext_tab_type_cnt;
            -- record the files url
            l_files_url := c_dir_list.files_url;
            -- first move the file to the target destination without changing the name
            -- because the file might be zipped or encrypted
            coreutils.copy_file (c_dir_list.arch_filepath,
                                 c_dir_list.pre_mv_filepath,
                                 SELF.DEBUG_MODE);
            -- decrypt the file if it's encrypted
            -- currently only supports gpg
            -- decrypt_file will return the decrypted filename
            -- IF the file isn't a recognized encrypted file type, it just returns the name passed
            l_filepath :=
               coreutils.decrypt_file (dirpath,
                                       c_dir_list.source_filename,
                                       SELF.passphrase,
                                       SELF.DEBUG_MODE);
            -- unzip the file if it's zipped
            -- currently will unzip, or gunzip, or bunzip2 or uncompress
            -- unzip_file will return the unzipped filename
            -- IF the file isn't a recognized zip archive file, it just returns the name passed
            l_filepath :=
                         coreutils.unzip_file (dirpath, c_dir_list.source_filename, SELF.DEBUG_MODE);
                 -- now move the file to the expected name
            -- do this with a copy/delete
            coreutils.copy_file (l_filepath, c_dir_list.filepath, SELF.DEBUG_MODE);
            o_app.log_msg ('File ' || c_dir_list.arch_filepath || ' copied to '
                           || c_dir_list.filepath);
            coreutils.delete_file (c_dir_list.pre_mv_filepath, SELF.DEBUG_MODE);
            -- get the number of lines in the file now that it is decrypted and uncompressed
            l_numlines :=
                       coreutils.get_numlines (SELF.DIRECTORY, c_dir_list.filename, SELF.DEBUG_MODE);
            -- get a total count of all the lines in all the files making up the external table
            l_sum_numlines := l_sum_numlines + l_numlines;
         END IF;

         -- WRITE an audit record for the file that was just archived
         o_app.set_action ('Audit feed');
         SELF.audit_file (p_source_filepath      => c_dir_list.source_filepath,
                          p_arch_filepath        => c_dir_list.arch_filepath,
                          p_filepath             => c_dir_list.filepath,
                          p_num_bytes            => c_dir_list.file_size,
                          p_num_lines            => l_numlines,
                          p_file_dt              => c_dir_list.file_dt,
                          p_validate             => CASE c_dir_list.ext_tab_ind
                             WHEN 'Y'
                                THEN TRUE
                             ELSE FALSE
                          END);
         -- IF we get this far, then we need to delete the source files
         -- this step is ignored if p_keep_source = TRUE
         o_app.set_action ('Delete source files');

         IF NOT p_keep_source
         THEN
            coreutils.delete_file (c_dir_list.source_filepath, SELF.DEBUG_MODE);
         END IF;
      END LOOP;

      -- check to see if the cursor was empty
      o_app.set_action ('Check for matching files');

      CASE
         WHEN NOT l_rows AND required = 'Y'
         THEN
            raise_application_error (o_app.get_err_cd ('no_files_found'),
                                     o_app.get_err_msg ('no_files_found'));
         WHEN NOT l_rows AND required = 'N'
         THEN
            o_app.log_msg ('No files found... but FILE_REQUIRED is "N"');
            -- empty out the contents of the file
            o_app.set_action ('Empty previous files');
            coreutils.delete_file (SELF.filepath);
            coreutils.create_file (SELF.filepath);
         ELSE
            -- matching files found, so ignore
                     -- alter the external table to contain all the files
            o_app.set_action ('Alter external table');

            BEGIN
               coreutils.ddl_exec (l_ext_tab_ddl, p_debug => SELF.DEBUG_MODE);
            EXCEPTION
               WHEN e_no_files
               THEN
                  raise_application_error (o_app.get_err_cd ('no_ext_files'),
                                           o_app.get_err_msg ('no_ext_files'));
            END;

            -- audit the external table
            o_app.set_action ('Audit external table');
            SELF.audit_ext_tab (p_num_lines => l_sum_numlines);
      END CASE;

      -- send the notification if configured
      o_app.set_action ('Send a notification');
      MESSAGE :=
            MESSAGE
         || CHR (10)
         || CHR (10)
         || 'The file'
         || CASE
               WHEN l_ext_file_cnt > 1
                  THEN 's'
               ELSE NULL
            END
         || ' can be downloaded at the following link:'
         || CHR (10)
         || l_files_url;

      IF l_numlines > 65536
      THEN
         MESSAGE :=
               MESSAGE
            || CHR (10)
            || CHR (10)
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      SELF.send;
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END process_feed;
END;
/