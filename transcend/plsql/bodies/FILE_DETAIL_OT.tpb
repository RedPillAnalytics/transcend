CREATE OR REPLACE TYPE BODY file_ot
AS

   -- constructor function for the FILE_DETAIL_OT object type
   -- some of the attributes can be overriden: both SOURCE_DIRECTORY and WORK_DIRECTORY
   CONSTRUCTOR FUNCTION file_detail_ot ( p_file_detail_id   NUMBER,
                                         p_directory        VARCHAR2(30) )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- load all the feed attributes
         SELECT file_detail_id, file_label, file_group, file_type, 
                directory, filename, lob_type, num_bytes, num_lines, file_dt, 
                characterset, store_files_native, compress_method, 
                encrypt_method, passphrase, file_clob, file_blob,
                processed_ts, session_id
           INTO self.file_detail_id, SELF.file_label, SELF.file_group, SELF.file_type,
                SELF.directory, SELF.filename, SELF.source_directory, SELF.source_filename,
                SELF.work_directory, SELF.lob_type, SELF.num_bytes, SELF.num_lines, SELF.file_dt, 
                SELF.characterset, SELF.store_files_native, SELF.compress_method,
                SELF.encrypt_method, SELF.passphrase, SELF.FILE_CLOB, SELF.FILE_BLOB,
                SELF.processed_ts, SELF.session_id              
           FROM (SELECT file_detail_id, file_label, file_group, file_type,
                        nvl( p_directory, directory ), source_filename filename,
                        CASE 
                        WHEN lower( store_files_native ) = 'no' AND self.characterset IS NOT NULL THEN 'clob' 
                        ELSE 'blob' 
                	END lob_type, num_bytes, num_lines, file_dt,
                        characterset, store_files_native, compress_method,
                        encrypt_method, passphrase, file_clob, file_blob,
                        processed_ts, session_id
                   FROM files_detail
                  WHERE file_detail_id = p_file_detail_id);
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
      l_dir_path := td_utils.get_dir_path (SELF.DIRECTORY);
            
      evolve.log_msg ('FILE_DETAIL confirmation completed successfully', 5);
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   -- retrieve a file from archive
   MEMBER PROCEDURE unarchive
   AS
      l_clob            CLOB;
      l_blob            BLOB;
      l_filename        files_detail.filename%type;
      l_max_linesize    NUMBER                  := 32767;
      l_buffersize      NUMBER;
      l_buffer          RAW(32767);
      l_amount          NUMBER;
      l_offset          NUMBER := 1;
      l_fh              utl_file.file_type;
      o_ev              evolve_ot               := evolve_ot( p_module => 'unarchive' );
   BEGIN
      
      -- open the file handle
      o_ev.change_action( 'open file handle' );
      l_fh := UTL_FILE.FOPEN( location => self.directory, 
                              filename => self.filename,
                              open_mode => 'w', 
                              max_linesize => l_max_linesize);
      
      -- get information about the LOB being used
      l_buffersize := CASE self.lob_type
                         WHEN 'clob' THEN DBMS_LOB.GETCHUNKSIZE( self.file_clob )
                         WHEN 'blob' THEN DBMS_LOB.GETCHUNKSIZE( self.file_blob )
                       END ;

                     
      -- use the smallest buffer we can
      l_buffersize := CASE WHEN l_buffersize < 32767 THEN l_buffersize ELSE 32767 END;         
      
      -- get the amount variable ready for the loop
      l_amount := l_buffersize;

      IF NOT evolve.is_debugmode
      THEN
      
         -- now, let write to the actual file
         CASE self.log_type
         -- this case is for working with a clob
         WHEN 'clob'
         THEN
            
            -- keep writing output as long as we are getting some from the file
            -- we know that we still have content as long as the amount read is the same as the buffer
            WHILE l_amount >= l_buffersize
            LOOP
               
               -- read into the buffer
               DBMS_LOB.READ( lob_loc    => self.file_clob,
                              amount     => l_amount,
                              offset     => l_offset,
                              buffer     => l_buffer);
               
               -- reset the offset based on the amount read in
               l_offset := l_offset + l_amount;
               
               -- now write the contents to the file
               UTL_FILE.PUT_RAW ( file      => l_fh,
                                  buffer    => l_buffer,
                                  autoflush => TRUE);

               -- flush the contents out to the file
               UTL_FILE.FFLUSH( file => l_fh );

            END LOOP;
            
            -- this is what we do when we have a blob       
         WHEN 'blob'
         THEN
            
            -- keep writing output as long as we are getting some from the file
            -- we know that we still have content as long as the amount read is the same as the buffer
            WHILE l_amount >= l_buffersize
            LOOP
               
               -- read into the buffer
               DBMS_LOB.READ( lob_loc    => self.file_blob,
                              amount     => l_amount,
                              offset     => l_offset,
                              buffer     => l_buffer);
               
               -- reset the offset based on the amount read in
               l_offset := l_offset + l_amount;
               
               -- now write the contents to the file
               UTL_FILE.PUT_RAW ( file      => l_fh,
                                  buffer    => l_buffer,
                                  autoflush => TRUE);

               -- flush the contents out to the file
               UTL_FILE.FFLUSH( file => l_fh );

            END LOOP;
            
         END CASE;

      END IF;      
      o_ev.clear_app_info;
   
   END unarchive;

END;
/

SHOW errors
