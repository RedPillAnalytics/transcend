CREATE OR REPLACE TYPE BODY file_ot
AS

   -- constructor function for the FILE_DETAIL_OT object type
   -- some of the attributes can be overriden: both SOURCE_DIRECTORY and WORK_DIRECTORY
   CONSTRUCTOR FUNCTION file_detail_ot ( p_file_detail_id   NUMBER )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- load all the feed attributes
         SELECT file_detail_id, file_label, file_group, file_type, 
                directory, filename, source_directory, source_filename, 
                work_directory, lob_type, num_bytes, num_lines, file_dt, 
                characterset, store_files_native, compress_method, 
                encrypt_method, passphrase, file_clob, file_blob,
                processed_ts, session_id
           INTO self.file_detail_id, SELF.file_label, SELF.file_group, SELF.file_type, 
                SELF.directory, SELF.filename, SELF.source_directory, SELF.source_filename,
                SELF.work_directory, SELF.lob_type, SELF.num_bytes, SELF.num_lines, SELF.file_dt, 
                SELF.characterset, SELF.store_files_native, SELF.compress_method,
                SELF.encrypt_method, SELF.passphrase, SELF.FILE_CLOB, SELF.FILE_BLOB,
                SELF.processed_ts, SELF.session_id		
           FROM (SELECT file_label, file_group, file_type, object_owner, object_name, DIRECTORY, filename,
                        work_directory, file_datestamp, min_bytes, max_bytes, baseurl, passphrase, 
			NVL( p_source_directory, source_directory),
                        source_regexp, match_parameter, source_policy, required, delete_source, delete_target,
                        reject_limit, 
			CASE 
			WHEN lower( store_files_native ) = 'no' AND self.characterset IS NOT NULL THEN 'clob' 
			ELSE 'blob' 
			END lob_type, store_files_native, characterset
                   FROM files_conf
                  WHERE REGEXP_LIKE (file_type, '^feed$', 'i') AND file_group = p_file_group
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
   END file_detail_ot;

   MEMBER PROCEDURE verify
   IS
      l_dir_path    all_directories.directory_path%TYPE;
      l_directory   all_external_tables.default_directory_name%TYPE;
      o_ev          evolve_ot                                         := evolve_ot (p_module => 'verify');
   BEGIN
      -- do checks to make sure all the provided information is legitimate
      -- check to see if the directories are legitimate

      -- if they aren't, the GET_DIR_PATH function raises an error
      l_dir_path := td_utils.get_dir_path (SELF.arch_directory);
      l_dir_path := td_utils.get_dir_path (SELF.source_directory);
      l_dir_path := td_utils.get_dir_path (SELF.DIRECTORY);

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
      
      evolve.log_msg ('FEED confirmation completed successfully', 5);
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;


   -- retrieve a file from archive
   MEMBER PROCEDURE unarchive(
      p_file_detail_id    NUMBER,
      p_directory         VARCHAR2 DEFAULT NULL
   )
   AS
      l_clob            CLOB;
      l_blob            BLOB;
      l_filename        files_detail.filename%type;
      l_max_linesize    NUMBER                  := 32767; 
      l_fh              utl_file.file_type;
      o_ev              evolve_ot               := evolve_ot( p_module => 'unarchive' );
   BEGIN

      -- get the row out of the files_detail table
      SELECT filename,
             file_clob,
             file_blob
        INTO l_filename,
             l_file_clob,
             l_file_blob
       WHERE file_detail_id = p_file_detail_id;
      
      -- open the file handle
      o_ev.change_action( 'open file handle' );
      l_fh := UTL_FILE.FOPEN( p_directory, l_filename,'w', max_linesize => 32767);


      
      -- do not want to store the file in the database if were are in debugmode
      -- might reconsider this after the fact

      IF NOT evolve.is_debugmode
      THEN
	       
	 -- oepn the source LOB to get ready to write it
	 DBMS_LOB.OPEN (l_src_lob, DBMS_LOB.lob_readonly);

	 CASE l_lob_type
	 WHEN 'clob'
	    THEN
	    DBMS_LOB.loadclobfromfile ( dest_lob          => l_dest_clob,
					src_bfile         => l_src_lob,
					amount            => DBMS_LOB.getlength(l_src_lob),
					dest_offset       => l_dst_offset,
					src_offset        => l_src_offset,
					bfile_csid        => NLS_CHARSET_ID( SELF.characterset ),
					lang_context      => l_lang_ctx,
					warning           => l_warning
                                      );
	    WHEN 'blob'
	       THEN
	       DBMS_LOB.loadblobfromfile ( dest_lob          => l_dest_blob,
					   src_bfile         => l_src_lob,
					   amount            => DBMS_LOB.getlength(l_src_lob),
					   dest_offset       => l_dst_offset,
					   src_offset        => l_src_offset
					 );
	       ELSE
	       evolve.raise_err( 'single_lob' );
	 END CASE;

	 -- now close the soure lob      
	 DBMS_LOB.CLOSE (l_src_lob);

         IF p_num_bytes >= max_bytes AND max_bytes <> 0
         THEN
	    o_ev.change_action( 'file too large');
            o_ev.send( p_label => file_label );
	    evolve.raise_err( 'file_too_large' );
         ELSIF p_num_bytes < min_bytes
         THEN
	    o_ev.change_action( 'file too small');
            o_ev.send( p_label => file_label );
	    evolve.raise_err( 'file_too_small' );
         END IF;

      END IF;

      o_ev.clear_app_info;
   END unarchive;

END;
/

SHOW errors
