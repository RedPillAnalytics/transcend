CREATE OR REPLACE TYPE BODY file_ot
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
      l_src_lob := BFILENAME ( CASE self.file_type WHEN 'feed' THEN l_source_directory ELSE SELF.directory END, l_filename );
      
      o_ev.change_action( 'Insert file detail' );
      
      
      -- INSERT into the FILE_DETAIL table to record the movement
      -- this is done regardless of runmode
      INSERT INTO files_detail
             ( file_detail_id, file_label, file_group,
               file_type, directory, filename, source_directory, 
	       source_filename, work_directory, num_bytes, num_lines, file_dt, file_clob, file_blob 
             )
	     VALUES ( files_detail_seq.NEXTVAL, file_label, file_group,
		      file_type, SELF.directory, l_filename, SELF.source_directory, p_source_filename, SELF.work_directory,
		      p_num_bytes, p_num_lines, p_file_dt, EMPTY_CLOB(), EMPTY_BLOB()
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

   -- store audit information about the feed or extract
   MEMBER PROCEDURE unarchive(
      p_file_detail_id    NUMBER,
      p_directory         VARCHAR2 DEFAULT NULL
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
      o_ev   evolve_ot := evolve_ot( p_module => 'unarchive' );
   BEGIN
      
      -- open the bfile
      o_ev.change_action( 'open BFILE' );
      l_src_lob := BFILENAME ( CASE self.file_type WHEN 'feed' THEN l_source_directory ELSE SELF.directory END, l_filename );
      
      o_ev.change_action( 'Insert file detail' );
      
      
      -- INSERT into the FILE_DETAIL table to record the movement
      -- this is done regardless of runmode
      INSERT INTO files_detail
             ( file_detail_id, file_label, file_group,
               file_type, directory, filename, source_directory, 
	       source_filename, work_directory, num_bytes, num_lines, file_dt, file_clob, file_blob 
             )
	     VALUES ( files_detail_seq.NEXTVAL, file_label, file_group,
		      file_type, SELF.directory, l_filename, SELF.source_directory, p_source_filename, SELF.work_directory,
		      p_num_bytes, p_num_lines, p_file_dt, EMPTY_CLOB(), EMPTY_BLOB()
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
   END unarchive;
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
