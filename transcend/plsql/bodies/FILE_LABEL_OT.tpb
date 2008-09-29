CREATE OR REPLACE TYPE BODY file_label_ot
AS

   -- store audit information about the feed or extract
   MEMBER PROCEDURE archive(
      p_num_bytes         NUMBER,
      p_num_lines         NUMBER,
      p_file_dt           DATE,
      p_filename          VARCHAR2 DEFAULT NULL,
      p_source_filename	  VARCHAR2 DEFAULT NULL,
      p_lob_type	  VARCHAR2 DEFAULT NULL
   )
   AS
      l_dest_clob    CLOB;
      l_dest_blob    BLOB;
      l_src_lob      BFILE;
      l_dst_offset   NUMBER := 1;
      l_src_offset   NUMBER := 1;
      l_lang_ctx     NUMBER := DBMS_LOB.default_lang_ctx;
      l_warning      NUMBER;
      l_filename          files_conf.filename%type         := NVL( p_filename, SELF.filename );
      -- determine which directory is the source directory for feeds
      l_source_directory  files_conf.source_directory%type := NVL( SELF.work_directory, SELF.source_directory );
      -- determine the lob_type
      l_lob_type     VARCHAR2(4) := NVL( p_lob_type, self.lob_type );
      o_ev   evolve_ot := evolve_ot( p_module => 'archive' );
   BEGIN
      
      -- open the bfile
      o_ev.change_action( 'open BFILE' );
      l_src_lob := BFILENAME ( CASE self.label_type WHEN 'feed' THEN l_source_directory ELSE SELF.directory END, l_filename );
      
      o_ev.change_action( 'Insert file detail' );
      
      
      -- INSERT into the FILE_DETAIL table to record the movement
      -- this is done regardless of runmode
      INSERT INTO files_detail
             ( file_detail_id, file_label, file_group,
               label_type, directory, filename, source_directory, 
	       source_filename, work_directory, num_bytes, num_lines, 
               file_dt, store_files_native, compress_method, encrypt_method, 
               passphrase, file_clob, file_blob 
             )
	     VALUES ( files_detail_seq.NEXTVAL, file_label, file_group,
		      label_type, SELF.directory, l_filename, SELF.source_directory, 
                      p_source_filename, SELF.work_directory, p_num_bytes, p_num_lines, 
                      p_file_dt, self.store_files_native, self.compress_method, self.encrypt_method,
                      self.passphrase, EMPTY_CLOB(), EMPTY_BLOB()
                    )
	     RETURNING file_clob, file_blob
	     INTO l_dest_clob, l_dest_blob;

      
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
   END archive;

   -- audits information about external tables after the file(s) have been put in place
   MEMBER PROCEDURE audit_object (p_num_lines NUMBER)
   IS
      l_num_rows         NUMBER         := 0;
      l_pct_miss         NUMBER;
      l_sql              VARCHAR2 (100);
      l_obj_name         VARCHAR2 (61)  := SELF.object_owner || '.' || SELF.object_name;
      e_data_cartridge   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_data_cartridge, -29913);
      e_no_object         EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_object, -942);
      e_no_files         EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_files, -1756);
      o_ev               evolve_ot      := evolve_ot (p_module => 'audit_object');
   BEGIN
      -- type object which handles logging and application registration for instrumentation purposes
      -- defaults to registering with DBMS_APPLICATION_INFO
      o_ev.change_action ('get count from object');
      l_sql := 'SELECT count(*) FROM ' || l_obj_name;
      evolve.log_msg ('Count SQL: ' || l_sql, 3);

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
            l_pct_miss := 100 - ((l_num_rows / p_num_lines) * 100);

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

         INSERT INTO files_obj_detail
                     (file_obj_detail_id, file_type, file_label, file_group,
                      object_owner, object_name, num_rows, num_lines, percent_diff
                     )
              VALUES (files_obj_detail_seq.NEXTVAL, SELF.file_type, SELF.file_label, SELF.file_group,
                      SELF.object_owner, SELF.object_name, l_num_rows, p_num_lines, l_pct_miss
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
