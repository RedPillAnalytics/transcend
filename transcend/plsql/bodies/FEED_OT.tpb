CREATE OR REPLACE TYPE BODY feed_ot
AS
   -- constructor function for the FEED_OT object type
   -- some of the attributes can be overriden: both SOURCE_DIRECTORY and WORK_DIRECTORY
   CONSTRUCTOR FUNCTION feed_ot(
      p_file_label	   VARCHAR2,
      p_source_directory   VARCHAR2
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- load all the feed attributes
         SELECT file_label, file_group, label_type, object_owner, object_name, DIRECTORY,
                filename,  CASE WHEN work_directory IS NOT NULL AND lower(work_directory) <> lower(source_directory)
		                THEN work_directory
		                ELSE NULL 
		            END work_directory,  min_bytes, max_bytes, baseurl,
                passphrase, source_directory, source_regexp, match_parameter, source_policy,
                required, delete_source, reject_limit, lob_type, store_files_native,
		characterset
           INTO SELF.file_label, SELF.file_group, SELF.label_type, SELF.object_owner, SELF.object_name, SELF.DIRECTORY,
                SELF.filename, SELF.work_directory, SELF.min_bytes, SELF.max_bytes, SELF.baseurl,
                SELF.passphrase, SELF.source_directory, SELF.source_regexp, SELF.match_parameter, SELF.source_policy,
                SELF.required, SELF.delete_source, SELF.reject_limit, SELF.lob_type, SELF.store_files_native,
		SELF.characterset
           FROM (SELECT file_label, file_group, label_type, object_owner, object_name, DIRECTORY, filename,
                        work_directory, min_bytes, max_bytes, baseurl, passphrase, 
			NVL( p_source_directory, source_directory),
                        source_regexp, match_parameter, source_policy, required, delete_source,
                        reject_limit, 
			CASE 
			WHEN lower( store_files_native ) = 'no' AND self.characterset IS NOT NULL THEN 'clob' 
			ELSE 'blob' 
			END lob_type, store_files_native, characterset
                   FROM file_conf
                  WHERE REGEXP_LIKE (label_type, '^feed$', 'i')
                        AND file_label = p_file_label);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_feed', p_file_label);
      END;

      -- run the business logic to make sure everything works out fine
      verify;
      -- return the self reference
      RETURN;
   END feed_ot;

   OVERRIDING MEMBER PROCEDURE verify
   IS
      l_dir_path    all_directories.directory_path%TYPE;
      l_directory   all_external_tables.default_directory_name%TYPE;
      o_ev          evolve_ot                                         := evolve_ot (p_module => 'verify');
   BEGIN
      -- do checks to make sure all the provided information is legitimate
      -- check to see if the directories are legitimate

      -- if they aren't, the GET_DIR_PATH function raises an error
      l_dir_path := td_utils.get_dir_path (SELF.source_directory);
      l_dir_path := td_utils.get_dir_path (SELF.DIRECTORY);

      IF self.work_directory IS NOT NULL
      THEN
	 l_dir_path := td_utils.get_dir_path (SELF.work_directory);
      END IF;

      -- if there is an external table associate with this feed
      -- we need to check a few things
      IF object_name IS NOT NULL
      THEN
         -- make sure the external table exists
         td_utils.check_table (p_owner => SELF.object_owner, p_table => SELF.object_name, p_external => 'yes');

         -- now need to find out what the directory is associated with the external table
         BEGIN
            -- get the directory from the external table
            SELECT default_directory_name
              INTO l_directory
              FROM all_external_tables
             WHERE owner = SELF.object_owner AND table_name = SELF.object_name;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               evolve.raise_err ('no_ext_tab', UPPER (SELF.object_owner || '.' || SELF.object_name));
         END;

         -- now compare the two and make sure they are the same
         IF UPPER (SELF.DIRECTORY) <> l_directory
         THEN
	    evolve.raise_err ('parms_not_compatible','The values specified for DIRECTORY must also be the location of the specified external table');
         END IF;
      END IF;

      -- also, make sure that the work_directory and directory are not the same
      IF SELF.directory = SELF.work_directory
      THEN
	 evolve.raise_err ('parms_not_compatible','The values specified for DIRECTORY and WORK_DIRECTORY cannot be the same');
      END IF;
      
      -- check that feeds with an associated external table have a characterset
      IF object_name IS NOT NULL AND characterset IS NULL
      THEN
	 evolve.raise_err ('parms_not_compatible','A feed with an associated external table must have a CHARACTERSET provided');
      END IF;

      -- check that feeds with a characterset don't have a STORE_FILES_NATIVE value of 'yes'
      -- thats because we don't have any way of knowing whether conversion will take place on a file or not
      -- part of having a characterset (specifying that a CLOB will be used) means storing files as they are after conversion
      IF characterset IS NULL AND td_core.is_true( store_files_native )
      THEN
	 evolve.raise_err ('parms_not_compatible','A feed with CHARACTERSET specified cannot store files in their original format');
      END IF;
      
      evolve.log_msg ('FEED confirmation completed successfully', 5);
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;
   OVERRIDING MEMBER PROCEDURE process
   IS
   -- is there an external table associated with this feed
      l_ext_tab_ind     BOOLEAN                            := CASE
         WHEN SELF.object_name IS NULL
            THEN FALSE
         ELSE TRUE
      END;
      l_ext_tab_yn	VARCHAR2(1) := CASE l_ext_tab_ind WHEN TRUE THEN 'Y' ELSE 'N' END;
	    -- is there a new filename associated with this feed
      l_filename_ind    BOOLEAN                            := CASE
         WHEN SELF.filename IS NULL
            THEN FALSE
         ELSE TRUE
      END;
      l_filename_yn	VARCHAR2(1) := CASE l_filename_ind WHEN TRUE THEN 'Y' ELSE 'N' END;
      -- is there a work directory or not
      l_workdir_exists  BOOLEAN				   := CASE WHEN self.work_directory IS NULL THEN FALSE ELSE TRUE END;
      
      l_rows_dirlist    BOOLEAN                            := FALSE;
      -- TO catch empty cursors
      l_numlines        NUMBER;
      l_max_numlines    NUMBER                             := 0;
      l_sum_numlines    NUMBER                             := 0;
      l_targ_file_cnt   NUMBER                             := 0;
      l_ext_tab         VARCHAR2 (61)                      := object_owner || '.' || object_name;
      l_filename	file_conf.filename%type;
      l_source_filename	file_conf.filename%type;
      l_working_filename	file_conf.filename%type;
      l_source_directory 	file_conf.source_directory%type := SELF.source_directory;
      l_filesize	NUMBER;
      l_blocksize	NUMBER;
      l_expanded	BOOLEAN;
      l_decrypted	BOOLEAN;
      l_loc_list	VARCHAR2(2000);
      l_url_list	VARCHAR2(2000);
      l_rows_delete	BOOLEAN		:= FALSE;
      e_no_files        EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_files, -1756);
      o_ev              evolve_ot                          := evolve_ot (p_module => 'process');
   BEGIN
      evolve.log_msg ('Processing feed "' || file_label || '"', 3);

      -- we are about to copy files to the target location
      -- but there might be an eternal table associated with these target files
      -- IF there is an external table, before copying in new files, we need to remove files placed in on previous runs
      -- we don't want the possiblity of data for a previous run getting loaded in
      -- later, if no new files are found, and the REQUIRED attribute is 'N' (meaning this file is not required)
      -- THEN we will create an empty file in it's place
      
      o_ev.change_action( 'delete current locations' );

      IF l_ext_tab_ind
      THEN	

	 FOR c_location IN ( SELECT  DIRECTORY, LOCATION
			       FROM dba_external_locations
			      WHERE owner = UPPER( object_owner ) AND table_name = UPPER( object_name )
			      ORDER BY LOCATION )
	 LOOP
            l_rows_delete := TRUE;
            td_utils.delete_file( c_location.DIRECTORY, c_location.LOCATION );
	 END LOOP;

	 IF l_rows_delete
	 THEN
            evolve.log_msg( 'Previous external table location files removed', 3 );
	 END IF;
	 
      END IF;

      -- now we need to see all the source files in the source directory that match the regular expression
      -- USE java stored procedure to populate global temp table DIR_LIST with all the files in the directory
      o_ev.change_action ('evaluate source directory');
      td_utils.directory_list ( l_source_directory );

      -- look at the contents of the DIR_LIST table to evaluate source files
      -- pull out only the ones matching the regular expression
      -- also work in a lot of the attributes to generate all the information needed for the object
      FOR c_dir_list IN
         (SELECT
                   -- name of each source file
                 source_filename,
		 -- name of the target file 
                 filename,
                   file_dt, file_size, targ_file_ind, targ_file_cnt,
                            
                   -- construct a file_url if BASEURL attribute is configured
                   -- this constructs a STRAGGED list of URL's if multiple files exist
                   -- otherwise it's null
                   REGEXP_REPLACE
                      (stragg (   SELF.baseurl
                            || '/'
                            || filename
                           ) OVER (PARTITION BY targ_file_ind),
                       ',',
                       CHR (10)
                      ) files_url
	    FROM (SELECT object_name, object_owner, source_filename, file_dt, file_size, targ_file_ind, targ_file_cnt,
			 CASE 
			 WHEN l_filename_yn = 'Y' AND targ_file_ind = 'Y' AND targ_file_cnt > 1
                         THEN REGEXP_REPLACE (SELF.filename, '\.', '_' || file_number || '.')
			 WHEN l_filename_yn = 'N' AND targ_file_ind = 'Y' AND targ_file_cnt = 1
			 THEN SELF.filename
			 ELSE filename END filename
              FROM (SELECT object_name, object_owner, source_filename, file_dt, file_size, targ_file_ind,
                           
                           -- rank gives us a number to use to auto increment files in case SOURCE_POLICY attribute is 'all'
                           ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY targ_file_ind DESC, source_filename) file_number,
                           
                           -- this gives us a count of how many files will be copied to the target
                           -- have this for each line
                           -- USE the TARG_FILE_IND derived in the select below
                           COUNT (*) OVER (PARTITION BY targ_file_ind) targ_file_cnt
                      FROM (SELECT              
				   -- the DIR_LIST table has a filename column
                                   -- we also have a filename attribute
                                   -- RENAME the filename from the DIR_LIST table as SOURCE_FILENAME
                                   filename source_filename,
                                   -- URL location if the target location is web enabled
                                   -- this is for notification purposes to send links for received files
                                   SELF.baseurl baseurl, 
				   file_dt, 
				   file_size,
                                   -- CASE statement determines TARG_FILE_IND
                                   -- this picks out the files that go to the target location
                                   -- uses the SOURCE_POLICY column to determine which ones go to target
                                   -- translated to a Y/N indicator based on the file date and the source_policy
                                   CASE
                                      WHEN LOWER (SELF.source_policy) = 'newest'
                                      AND file_dt = MAX (file_dt) OVER (PARTITION BY 1)
                                         THEN 'Y'
                                      WHEN LOWER (SELF.source_policy) = 'oldest'
                                      AND file_dt = MIN (file_dt) OVER (PARTITION BY 1)
                                         THEN 'Y'
                                      WHEN LOWER (SELF.source_policy) = 'all'
                                         THEN 'Y'
                                      ELSE 'N'
                                   END targ_file_ind
                              FROM dir_list
                             -- matching regexp and match_parameter to find matching source files
                            WHERE  REGEXP_LIKE (filename, SELF.source_regexp, SELF.match_parameter)))
		   ORDER BY targ_file_ind ASC))
      LOOP

	 -- get the source filename we are working with and store it in two variable
	 l_source_filename    := c_dir_list.source_filename;
	 l_working_filename   := l_source_filename;

	 -- get the target filename we are working with and store in a variable
	 l_filename := c_dir_list.filename;
	 
         evolve.log_msg ('Processing file ' || l_source_filename || ' in directory '||l_source_directory, 3);

         -- catch empty cursor sets
         l_rows_dirlist := TRUE;
         -- reset variables used in the cursor
         l_numlines := 0;	 
	
	 -- if this feed uses a work_directory (l_workdir_exists variable)
	 -- then we need to copy the file to the work directory
	 IF l_workdir_exists
	 THEN
            td_utils.copy_file ( p_source_directory => l_source_directory, 
				 p_source_filename  => l_source_filename,
			         p_directory	    => SELF.work_directory,
				 p_filename	    => l_source_filename 
			       );
	    -- we are now concerned with the work_directory as the source_directory
	    l_source_directory := self.work_directory;
	 END IF;

         o_ev.change_action ( 'archive feed' );
         -- now, we need to know whether the files is archived prior to any conversion process
         -- by conversion, I mean expanding or decrypting, or both
         -- if STORE_FILES_NATIVE is true, then we archive them however they came in
         -- these will be stored as blob, not clobs
         IF td_core.is_true( self.store_files_native ) AND NOT evolve.is_debugmode
         THEN
	    -- this writes auditing information in the repository
	    -- also stores the file in the database
            SELF.archive ( p_filename             => l_filename,
			   p_source_filename      => l_working_filename,
			   p_num_bytes            => l_filesize,
			   p_num_lines            => l_numlines,
			   p_file_dt              => c_dir_list.file_dt
                         );
         END IF;
	    
	    
	 -- now, specifically working on compression
	 -- if we have a valid compression method, meaning SELF.compress_method is not null
	 IF self.compress_method IS NOT NULL
	 THEN
	    -- we need to expand the file
	    td_utils.expand_file( p_directory => l_source_directory, 
				  p_filename  => l_source_filename, 
				  r_filename  => l_working_filename,
				  r_filesize  => l_filesize,
				  r_blocksize => l_blocksize,
				  r_expanded  => l_expanded,
				  p_comp_method => self.compress_method );
	 END IF;
	 
	 
	 -- now, specifically working on decryption
	 -- if we have a valid encryption method, meaning SELF.decrypt_method is not null
	 IF self.encrypt_method IS NOT NULL
	 THEN
	    -- we need to decrypt the file
	    td_utils.decrypt_file( p_directory => l_source_directory, 
				   p_filename  => l_source_filename,
				   p_passphrase => self.passphrase, 
				   r_filename  => l_working_filename,
				   r_filesize  => l_filesize,
				   r_blocksize => l_blocksize,
				   r_decrypted => l_decrypted,
				   p_encrypt_method => self.encrypt_method );
	 END IF;
	 
	 
	 -- get the number of lines in the file now that the file has been decrypted and expanded
	 -- otherwise, these values don't make any sense
	 l_numlines := td_utils.get_numlines ( l_source_directory, l_working_filename );

         o_ev.change_action ( 'archive feed' );
         -- for all the files that are converted prior to archival, here is another archival
         -- if STORE_FILES_NATIVE is not true, then we archive them now
         IF NOT td_core.is_true( self.store_files_native ) AND NOT evolve.is_debugmode
         THEN
	    -- this writes auditing information in the repository
	    -- also stores the file in the database
            SELF.archive ( p_filename             => l_filename,
			   p_source_filename      => l_working_filename,
			   p_num_bytes            => l_filesize,
			   p_num_lines            => l_numlines,
			   p_file_dt              => c_dir_list.file_dt
                         );
         END IF;
         

         IF c_dir_list.targ_file_ind = 'Y'
         THEN
	    o_ev.change_action ('process target files');

            -- get a total count of all the lines in all the files moving to target
            l_sum_numlines := l_sum_numlines + l_numlines;

            -- see if this is the maximum line number size
            -- if it is, then keep it
            IF l_numlines > l_max_numlines
            THEN
               l_max_numlines := l_numlines;
            END IF;

	    -- RECORD the number of target files
            -- this count will be the same no matter which of the rows we pull it from, as analytics calculated it
            -- might as well use the last
            l_targ_file_cnt := c_dir_list.targ_file_cnt;
            
	    -- the file now needs to be put into the target location
	    -- if the source is the work directory, then this is a rename
	    -- if not, it needs to be a copy
	    IF l_workdir_exists
	    THEN
	       o_ev.change_action ('copy to target location');
	       td_utils.copy_file( l_source_directory, l_working_filename, self.directory, l_filename );
	    ELSE
	       o_ev.change_action ('move to target location');
	       BEGIN
		  td_utils.move_file( l_source_directory, l_working_filename, self.directory, l_filename );
	       EXCEPTION
		  WHEN td_utils.different_filesystems
		  THEN
		     evolve.raise_err( 'work_dir_fs' );
	       END;

	    END IF;
	    
	    -- add the filename to the ALTER EXTERNAL TABLE location list
	    -- this will be used to alter the external table (possibly) when the loop is complete
	    l_loc_list := l_loc_list ||CASE l_loc_list WHEN NULL THEN NULL ELSE ',' end|| self.directory || ':' || l_working_filename;
	    
	    -- add the filename to the URL list
	    l_url_list := l_url_list ||CASE l_url_list WHEN NULL THEN NULL ELSE CHR(10) end|| self.baseurl || '/' || l_working_filename;


         END IF;

         -- IF we get this far, then we need to delete the source files
         -- this step is ignored if delete_source = 'no'
         o_ev.change_action ('delete source files');

         IF td_core.is_true (delete_source)
         THEN
            td_utils.delete_file (SELF.source_directory, l_source_filename);
         END IF;
	 
      END LOOP;

      -- series of debug statements
      o_ev.change_action ('check for matching files');
      evolve.log_msg ('Attribute REQUIRED is: ' || required, 5);
      evolve.log_msg ('Attribute SOURCE_POLICY is: ' || source_policy, 5);
      evolve.log_msg ('The number of files moved to target: ' || l_targ_file_cnt, 5);
      evolve.log_msg ('Variable L_ROWS_DIRLIST is: ' || CASE
                         WHEN l_rows_dirlist
                            THEN 'TRUE'
                         ELSE 'FALSE'
                      END, 5);

      CASE
         -- there were no files found, and the file is required
         -- then we should fail
      WHEN NOT l_rows_dirlist AND td_core.is_true (SELF.required)
         THEN
            evolve.raise_err ('no_files_found');
         -- there were no files found
         -- however, the REQUIRED attribute is "no"
	 -- and, there is a configured external table
         -- therefore, any load process dependent on this job should proceed
         -- but need a "business logic" way of saying "no rows for today"
         -- so I empty the file out
         -- an external table with a zero-byte file gives "no rows returned"
      WHEN NOT l_rows_dirlist AND NOT td_core.is_true (required) AND l_ext_tab_ind
         THEN
            evolve.log_msg ('No files found... but none are required', 3);
            o_ev.change_action ('empty previous files');

            FOR c_location IN (SELECT DIRECTORY, LOCATION
                                 FROM dba_external_locations
                                WHERE owner = UPPER (object_owner) AND table_name = UPPER (object_name))
            LOOP
               td_utils.create_file (c_location.DIRECTORY, c_location.LOCATION);
            END LOOP;
         WHEN l_rows_dirlist AND l_targ_file_cnt > 0
         -- matching files found, and the number of location files is greater than zero
         -- alter the external table to contain all the files
      THEN
            o_ev.change_action ('alter external table');

            BEGIN
               evolve.exec_sql (p_sql => 'alter table ' ||l_ext_tab||' location ('|| l_loc_list ||')', p_auto => 'yes');
               evolve.log_msg ('External table ' || l_ext_tab || ' altered', 3);
            EXCEPTION
               WHEN e_no_files
               THEN
                  evolve.raise_err ('no_ext_files', l_ext_tab);
            END;

            -- audit the external table
            o_ev.change_action ('audit external table');
            SELF.audit_object (p_num_lines => l_sum_numlines);
         WHEN l_rows_dirlist AND l_targ_file_cnt = 0
              -- matching files found, but there were no location files
              -- there were files found at the OS level
         -- however, no files deemed as possible locations
         -- the only explanation for this is that there are multiple files found
         -- and the SOURCE_POLICY is 'fail'
      THEN
            o_ev.change_action ('fail source policy enacted');
            evolve.raise_err ('fail_source_policy');
         ELSE
            NULL;
      END CASE;

      -- notify about successful arrival of feed
      o_ev.change_action ('notify success');
      SELF.announce (p_num_files => l_targ_file_cnt, p_num_lines => l_max_numlines, p_files_url => l_url_list);
      o_ev.clear_app_info;
   END process;
END;
/

SHOW errors