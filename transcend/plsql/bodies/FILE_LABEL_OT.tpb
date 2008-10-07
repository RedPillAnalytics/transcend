CREATE OR REPLACE TYPE BODY file_label_ot
AS

   -- store audit information about the feed or extract
   MEMBER FUNCTION archive(
      p_loc_directory      VARCHAR2,
      p_loc_filename       VARCHAR2,
      p_directory          VARCHAR2,
      p_filename           VARCHAR2,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_filename    VARCHAR2 DEFAULT NULL,
      p_file_dt            DATE     DEFAULT NULL
   )
      RETURN NUMBER 
   AS
      l_detail_id    file_detail.file_detail_id%type;
      l_dest_blob    BLOB;
      l_src_lob      BFILE;
      l_dst_offset   NUMBER := 1;
      l_src_offset   NUMBER := 1;
      l_lang_ctx     NUMBER := DBMS_LOB.default_lang_ctx;
      l_warning      NUMBER;

      -- variables to hold attributes about the file
      l_file_exists  BOOLEAN;
      l_filesize     NUMBER;
      l_blocksize    NUMBER;
      l_numlines     NUMBER;

      -- variable for ease of use
      l_file_dir    VARCHAR2( 100 ) := p_loc_filename||' in directory '||p_loc_directory;

      -- evolve instrumentation object
      o_ev   evolve_ot := evolve_ot( p_module => 'archive' );
   BEGIN
      
      evolve.log_msg( 'Archiving file '||l_file_dir );

      IF NOT evolve.is_debugmode
      THEN

         -- get information about the file
         evolve.log_msg( 'Getting information about file from UTL_FILE ', 4 );
         UTL_FILE.fgetattr( location     => p_loc_directory, 
                            filename     => p_loc_filename, 
                            fexists      => l_file_exists, 
                            file_length  => l_filesize, 
                            block_size   => l_blocksize );
         
      ELSE
         -- set values for DEBUGMODE
         l_filesize := 0;
         l_blocksize := 0;
      END IF;

      -- get number of lines in the file
      -- in DEBUGMODE returns 0
      l_numlines := td_utils.get_numlines( p_directory  => p_loc_directory,
                                          p_filename   => p_loc_filename );
      
      IF NOT evolve.is_debugmode
      THEN

         -- open the bfile
         o_ev.change_action( 'open BFILE' );
         l_src_lob := BFILENAME ( p_loc_directory, p_loc_filename );
         
      END IF;
         
      o_ev.change_action( 'Insert file detail' );

      -- INSERT into the FILE_DETAIL table to record the movement
      -- this is done regardless of runmode
      evolve.log_msg( 'Inserting information into FILE_DETAIL', 4 );
      INSERT INTO file_detail
             ( file_detail_id, file_label, file_group,
               label_type, directory, filename, source_directory, 
	       source_filename, num_bytes, num_lines, 
               file_dt, store_original_files, compress_method, encrypt_method, 
               passphrase, label_file
             )
       VALUES ( file_detail_seq.NEXTVAL, file_label, file_group,
		label_type, p_directory, p_filename, p_source_directory, 
                p_source_filename, l_filesize, l_numlines,
                p_file_dt, self.store_original_files, self.compress_method, self.encrypt_method,
                self.passphrase, EMPTY_BLOB()
              )
       RETURNING label_file, file_detail_id
         INTO l_dest_blob, l_detail_id;
      
      IF NOT evolve.is_debugmode
      THEN

         -- open the source LOB to get ready to write it
	 DBMS_LOB.OPEN (l_src_lob, DBMS_LOB.lob_readonly);

	 DBMS_LOB.loadblobfromfile ( dest_lob          => l_dest_blob,
				     src_bfile         => l_src_lob,
				     amount            => DBMS_LOB.getlength(l_src_lob),
				     dest_offset       => l_dst_offset,
				     src_offset        => l_src_offset
				   );

	 -- now close the soure lob      
	 DBMS_LOB.CLOSE (l_src_lob);

      END IF;

      o_ev.clear_app_info;
      RETURN l_detail_id;
   END archive;

   -- store audit information about the feed or extract
   MEMBER PROCEDURE modify_archive(
      p_file_detail_id     NUMBER,
      p_loc_directory      VARCHAR2,
      p_loc_filename       VARCHAR2,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_filename    VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_file_dt            DATE     DEFAULT NULL
   )
   AS
      -- variables to hold attributes about the file
      l_file_exists  BOOLEAN;
      l_filesize     NUMBER;
      l_blocksize    NUMBER;
      l_numlines     NUMBER;
   
      -- variable for ease of use
      l_file_dir    VARCHAR2( 100 ) := p_loc_filename||' in directory '||p_loc_directory; 

      o_ev           evolve_ot      := evolve_ot( p_module => 'modify_archive' );
   BEGIN
      
      IF NOT evolve.is_debugmode
      THEN

         -- get information about the file
         evolve.log_msg( 'Getting information about file from UTL_FILE ', 4 );
         UTL_FILE.fgetattr( location      => p_loc_directory, 
                            filename      => p_loc_filename, 
                            fexists       => l_file_exists, 
                            file_length   => l_filesize, 
                            block_size    => l_blocksize );
         
      ELSE
         -- set values for DEBUGMODE
         l_filesize := 0;
         l_blocksize := 0;
      END IF;

      -- get number of lines in the file
      -- in DEBUGMODE returns 0
      l_numlines := td_utils.get_numlines( p_directory  => p_loc_directory,
                                           p_filename   => p_loc_filename );
      
      -- update attributes in the FILE_DETAIL table as necessary
      UPDATE file_detail
         SET num_bytes = l_filesize,
             num_lines = l_numlines,
	     source_directory = NVL( p_source_directory, source_directory ),
	     source_filename = NVL( p_source_filename, source_filename ),
	     directory = NVL( p_directory, directory ),
	     filename = NVL( p_filename, filename ),
	     file_dt = NVL( p_file_dt, file_dt )
       WHERE file_detail_id = p_file_detail_id;

      o_ev.clear_app_info;
   END modify_archive;
   
   -- audits information about external tables after the file(s) have been put in place
   MEMBER PROCEDURE audit_object ( p_file_detail_id NUMBER )
   IS
      l_num_rows         NUMBER         := 0;
      l_pct_miss         NUMBER;
      l_detail_id        file_detail.file_detail_id%type;
      l_sql              VARCHAR2 (100);
      l_obj_name         VARCHAR2 (61)  := SELF.object_owner || '.' || SELF.object_name;
      l_numlines         NUMBER;
      e_data_cartridge   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_data_cartridge, -29913);
      e_no_object         EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_object, -942);
      e_no_files         EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_files, -1756);
      o_ev               evolve_ot      := evolve_ot (p_module => 'file_label_ot.audit_object');
   BEGIN
      -- type object which handles logging and application registration for instrumentation purposes
      -- defaults to registering with DBMS_APPLICATION_INFO
      o_ev.change_action ('get count from object');
      l_sql := 'SELECT count(*) FROM ' || l_obj_name;
      evolve.log_msg ('Count SQL: ' || l_sql, 3);
      
      -- get the number of lines in the file
      -- i get this by looking at file_detail
      
      SELECT num_lines
        INTO l_numlines
        FROM file_detail
       WHERE file_detail_id = p_file_detail_id;

      IF NOT evolve.is_debugmode
      THEN
         BEGIN
            EXECUTE IMMEDIATE l_sql
                         INTO l_num_rows;
         EXCEPTION
            WHEN e_data_cartridge
            THEN
               -- no matter what happens, we want to log the error
               -- this is prior to the case on purpose
               evolve.log_err;

                    -- use a regular expression to pull the KUP error out of SQLERRM
               -- this tells us the explicit issue with the external table
               CASE REGEXP_SUBSTR (SQLERRM, '^KUP-[[:digit:]]{5}', 1, 1, 'im')
                       -- so far, only one known error to check for
                  -- others will come
               WHEN 'KUP-04040'
                  THEN
                     o_ev.change_action ('external file missing');
                     o_ev.send (p_label => SELF.file_label);
                     o_ev.clear_app_info;
                     evolve.raise_err ('ext_file_missing', l_obj_name);
                  -- All other errors get routed here
               ELSE
                     o_ev.clear_app_info;
                     evolve.raise_err ('data_cartridge', l_obj_name);
               END CASE;
         END;

         BEGIN
            -- calculate the percentage difference
            l_pct_miss := 100 - ((l_num_rows / l_numlines) * 100);
            
            IF l_pct_miss > reject_limit
            THEN
               o_ev.change_action ('reject limit exceeded');
               -- notify if reject limit is exceeded
               o_ev.send (p_label => SELF.file_label);
               o_ev.clear_app_info;
               evolve.raise_err ('reject_limit_exceeded');
            END IF;

         EXCEPTION
            WHEN ZERO_DIVIDE
            THEN
               evolve.log_msg ('A SELECT from the specified object returns no rows', 3);
         END;

         INSERT INTO file_object_detail
                     (file_object_detail_id, label_type, file_label, file_group,
                      object_owner, object_name, num_rows, num_lines, percent_diff
                     )
              VALUES (file_object_detail_seq.NEXTVAL, SELF.label_type, SELF.file_label, SELF.file_group,
                      SELF.object_owner, SELF.object_name, l_num_rows, l_numlines, l_pct_miss
                     );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN e_no_object
      THEN
         evolve.raise_err ('no_obj', SELF.object_owner || '.' || SELF.object_name);
   END audit_object;

   MEMBER PROCEDURE announce(
      p_files_url   VARCHAR2,
      p_num_lines   NUMBER,
      p_num_files   NUMBER DEFAULT 1
   )
   AS
      o_ev        evolve_ot := evolve_ot( p_module => 'announce' );
      l_message   notification_events.message%type;
   BEGIN
      -- notify about successful arrival of feed
      o_ev.change_action( 'Notify success' );
      l_message :=
            'The file'
         || CASE
               WHEN p_num_files > 1
                  THEN 's'
               ELSE NULL
            END
         || ' can be downloaded at the following link'
         || CASE
               WHEN p_num_files > 1
                  THEN 's'
               ELSE NULL
            END
         || ':'
         || CHR( 10 )
         || p_files_url;

      IF p_num_lines > 65536
      THEN
         l_message :=
               l_message
            || CHR( 10 )
            || CHR( 10 )
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      o_ev.send( p_label   => self.file_label,
		 p_message => l_message );

   END announce;

END;
/

SHOW errors
