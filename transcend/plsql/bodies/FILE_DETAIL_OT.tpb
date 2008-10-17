CREATE OR REPLACE TYPE BODY file_detail_ot
AS

   -- constructor function for the FILE_DETAIL_OT object type
   CONSTRUCTOR FUNCTION file_detail_ot ( 
      p_file_detail_id   NUMBER,
      p_directory        VARCHAR2 DEFAULT NULL 
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- load all the feed attributes
         SELECT file_detail_id, file_label, file_group, label_type, 
                nvl( p_directory, directory), filename, source_directory, source_filename,
                archive_filename, num_bytes, num_lines, file_dt, 
                store_original_files, compress_method, 
                encrypt_method, passphrase, label_file,
                processed_ts, session_id
           INTO self.file_detail_id, SELF.file_label, SELF.file_group, SELF.file_type,
                SELF.directory, SELF.filename, SELF.source_directory, SELF.source_filename,
                SELF.archive_filename, SELF.num_bytes, SELF.num_lines, SELF.file_dt, 
                SELF.store_original_files, SELF.compress_method,
                SELF.encrypt_method, SELF.passphrase, SELF.label_file,
                SELF.processed_ts, SELF.session_id
           FROM file_detail
          WHERE file_detail_id = p_file_detail_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_file_detail', p_file_detail_id);
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
      o_ev          evolve_ot                                         := evolve_ot (p_module => 'file_detail_ot.verify');
   BEGIN
      -- do checks to make sure all the provided information is legitimate

      -- check to see if the directories are legitimate
      -- if they aren't, the GET_DIR_PATH function raises an error
      l_dir_path := td_utils.get_dir_path (SELF.DIRECTORY);
            
      evolve.log_msg ('FILE_DETAIL confirmation completed successfully', 5);
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   MEMBER PROCEDURE inspect(
      p_max_bytes   NUMBER,
      p_min_bytes   NUMBER
   )
   IS
      o_ev          evolve_ot    := evolve_ot (p_module => 'file_detail_ot.inspect');
   BEGIN

      -- check and make sure the file sizes are legitimate
      IF self.num_bytes >= p_max_bytes AND p_max_bytes IS NOT NULL
      THEN
	 o_ev.change_action( 'file too large');
         o_ev.send( p_label => file_label );
	 evolve.raise_err( 'file_too_large' );
      ELSIF self.num_bytes < p_min_bytes AND p_max_bytes IS NOT NULL
      THEN
	 o_ev.change_action( 'file too small');
         o_ev.send( p_label => file_label );
	 evolve.raise_err( 'file_too_small' );
      END IF;

      -- reset the evolve_object
      o_ev.clear_app_info;
   END inspect;

   -- retrieve a file from archive
   MEMBER PROCEDURE unarchive
   AS
      l_blob            BLOB;
      l_max_linesize    NUMBER                  := 32767;
      l_buffersize      NUMBER;
      l_buffer          RAW(32767);
      l_amount          NUMBER;
      l_offset          NUMBER := 1;
      l_fh              utl_file.file_type;
      l_file            VARCHAR2(61)            := upper(self.directory)||':'|| self.archive_filename;
      o_ev              evolve_ot               := evolve_ot( p_module => 'unarchive_file_detail' );
   BEGIN

      evolve.log_msg( 'Unarchiving file to '|| l_file );
      
      -- open the file handle
      o_ev.change_action( 'open file handle' );
      l_fh := UTL_FILE.FOPEN( location => self.directory, 
                              filename => self.archive_filename,
                              open_mode => 'wb', 
                              max_linesize => l_max_linesize);

      o_ev.change_action( 'Get LOB information' );      
      -- get information about the LOB being used
      l_buffersize := DBMS_LOB.GETCHUNKSIZE( self.label_file ) ;
      evolve.log_msg( 'Chunksize: '||l_buffersize, 5 );
                     
      -- use the smallest buffer we can
      l_buffersize := CASE WHEN l_buffersize < 32767 THEN l_buffersize ELSE 32767 END;         
      evolve.log_msg( 'Buffersize: '||l_buffersize, 5 );
      
      -- get the amount variable ready for the loop
      l_amount := l_buffersize;

      IF NOT evolve.is_debugmode
      THEN
         
         -- keep writing output as long as we are getting some from the file
         -- we know that we still have content as long as the amount read is the same as the buffer
         WHILE l_amount >= l_buffersize
         LOOP
   
            o_ev.change_action( 'Read LOB' );      
            
            -- read into the buffer
            DBMS_LOB.READ( lob_loc    => self.label_file,
                           amount     => l_amount,
                           offset     => l_offset,
                           buffer     => l_buffer);
            
            -- reset the offset based on the amount read in
            l_offset := l_offset + l_amount;

            o_ev.change_action( 'Write LOB' );      
            
            -- now write the contents to the file
            UTL_FILE.PUT_RAW ( file      => l_fh,
                               buffer    => l_buffer,
                               autoflush => TRUE);
            
            o_ev.change_action( 'Flush LOB' );      
            -- flush the contents out to the file
            UTL_FILE.FFLUSH( file => l_fh );

         END LOOP;

         -- close the file handle         
         utl_file.fclose( l_fh );

      END IF;      
      o_ev.clear_app_info;
   
   END unarchive;

END;
/

SHOW errors
