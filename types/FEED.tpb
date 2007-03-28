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
      -- type object which handles logging and application registration for instrumentation purposes
      -- defaults to registering with DBMS_APPLICATION_INFO
      o_app.set_action ('Get count from table');
      l_sql := 'SELECT count(*) FROM ' || SELF.object_owner || '.' || SELF.object_name;

      -- translates the Y/N attribute into a boolean
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

      -- look at the contents of the DIR_LIST table to evaluate source files
      FOR c_dir_list IN
         
         -- subquery factoring clause gives us external table, owner and current files
         -- these will be joined in with proposed files to be moved into location
         -- this tells us whether we will need to delete some files after the file movement
         -- also tells us whether we will need to alter the external table after completion
         (WITH current_ext_files AS
               (SELECT owner,
                       table_name,
                       -- this is the absolute path of all files currently in the external table
                       coreutils.get_dir_path (SELF.DIRECTORY) || '/' || LOCATION cef_filepath
--                       LOCATION cef_filename
                  FROM dba_external_tables JOIN dba_external_locations USING (owner, table_name)
                       )
          SELECT   object_name,                                              -- external table owner
                   object_owner,                                              -- external table name
                   source_filename,                                     -- name of each source files
                   source_filepath,                               -- name converted to absolute path
                   CASE
                      -- use analytics to determine how many files are going into place
                      -- that tells us whether to increment the filenames
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
                   cef_filepath,
--                   cef_filename
                   file_dt,
                   file_size,
                   ext_tab_ind,
                   ext_tab_type_cnt,
                      
                      -- use analytics (stragg function) to construct the alter table command (if needed)
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
                             -- construct a file_url if BASEURL attribute is configured
                   -- this constructs a STRAGGED list of URL's if multiple files exist
                             -- otherwise it's null
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
              FROM (SELECT object_name,
                           object_owner,
                           source_filename,
                           source_filepath,
                           filepath,
                           pre_mv_filepath,
                           arch_filepath,
                           file_dt,
                           file_size,
                           ext_tab_ind,
                           -- rank gives us a number to use to auto increment files in case SOURCE_POLICY attribute is 'all'
                           RANK () OVER (PARTITION BY 1 ORDER BY ext_tab_ind DESC,
                            source_filename) file_number,
                                          -- this gives us a count of how many files will be copied into the external table
                                          -- have this for each line
                           -- use the EXT_TAB_IND derived in the select below
                           COUNT (*) OVER (PARTITION BY ext_tab_ind) ext_tab_type_cnt
                      FROM (SELECT                       -- the dir_list table has a filename column
                                   -- we also have a filename attribute
                                   -- rename the filename from the table as SOURCE_FILENAME
                                   filename source_filename,
                                   -- URL location if the target location is web enabled
                                   -- this is for notification purposes to send links for received files
                                   SELF.baseurl baseurl,
                                   -- translate directory objects and filenames to absolute paths
                                   -- because Java actually does most the heavy lifting
                                   -- and java doesn't know anything about a directory object
                                   -- path of the target object (if there's only one)
                                   SELF.filepath filepath,
                                   -- path to the source file
                                   SELF.source_dirpath || '/' || filename source_filepath,
                                   -- path to an intermediate file location just prior to being placed in the external table
                                   -- this is so the file can be decompressed and decrypted AFTER the move
                                   SELF.dirpath || '/' || filename pre_mv_filepath,
                                   -- use the attribute FILE_DATESTAMP to determin if the archived file needs a date added
                                   -- some files will come in dated already, and two datestamps are silly
                                   -- when we have a whole catalog to track the files
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
                                   -- case statement determines an EXT_TAB_IND
                                   -- this picks out the files that will go to the external table
                                   -- uses the SOURCE_POLICY column to determine which ones to get
                                   -- that is translated to a Y/N indicator based on the date of the file
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
                                   END ext_tab_ind,
                                   UPPER (SELF.object_name) object_name,
                                   UPPER (SELF.object_owner) object_owner
                              FROM dir_list
                             -- matching regexp and regexp_options to find matching source files
                            WHERE  REGEXP_LIKE (filename, SELF.source_regexp, SELF.regexp_options))) dl
                 left JOIN
                   -- joining the subquery factoring clause
                   -- this compares proposed files to the existing files
                   current_ext_files cef
                   ON dl.object_owner = cef.owner
                 AND dl.object_name = cef.table_name
                 AND dl.filepath = cef_filepath
          ORDER BY ext_tab_ind ASC)
      LOOP
         BEGIN
            -- catch empty cursor sets
            l_rows := TRUE;
       -- use a full outer join between proposed external table files and current ones
       -- I'll get rows back where most the columns are empty
       -- these would have come from the subquery factoring clause
       -- this means there are location files for the external table that don't match incoming files
       -- these need to be deleted, so it's the first thing I do, and then RETURN from the block
       -- IF c_dir_list.source_filename IS NULL
--             THEN
--                coreutils.delete_file (DIRECTORY, c_dir_list.cef_filename, SELF.DEBUG_MODE);
--           -- this will take be out of this block to the next item in the loop
--           RETURN;
--             END IF;

            -- reset variables used in the cursor
            l_numlines := 0;
            -- copy file to the archive location
            o_app.set_action ('Copy archivefile');
            coreutils.copy_file (c_dir_list.source_filepath,
                                 c_dir_list.arch_filepath,
                                 SELF.DEBUG_MODE);
            o_app.log_msg ('Archive file ' || c_dir_list.arch_filepath || ' created');
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
               coreutils.delete_file (DIRECTORY, c_dir_list.source_filename, SELF.DEBUG_MODE);
               o_app.log_msg (   'File '
                              || c_dir_list.source_filepath
                              || ' moved to '
                              || c_dir_list.filepath);
--            coreutils.delete_file (c_dir_list.pre_mv_filepath, SELF.DEBUG_MODE);
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
               coreutils.delete_file (source_directory, c_dir_list.source_filename,
                                      SELF.DEBUG_MODE);
            END IF;
         END;
      END LOOP;

      -- check to see if the cursor was empty
      o_app.set_action ('Check for matching files');

      CASE
         WHEN NOT l_rows AND required = 'Y'
         THEN
            raise_application_error (o_app.get_err_cd ('no_files_found'),
                                     o_app.get_err_msg ('no_files_found'));
         -- there were no matching files for this configuration
         -- however, the REQUIRED attribute is N
         -- therefore, and load process dependent on this job will proceed
         -- but need a "business logic" way of saying "no rows for today"
         -- so I empty the file out
         -- an external table with a zero-byte file gives "no rows returned"
      WHEN NOT l_rows AND required = 'N'
         THEN
            o_app.log_msg ('No files found... but none are required');
            o_app.set_action ('Empty previous files');

            FOR c_location IN (SELECT DIRECTORY,
                                      LOCATION
                                 FROM dba_external_locations
                                WHERE owner = UPPER (object_owner)
                                  AND table_name = UPPER (object_name))
            LOOP
               coreutils.create_file (c_location.DIRECTORY, c_location.LOCATION, SELF.DEBUG_MODE);
            END LOOP;
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