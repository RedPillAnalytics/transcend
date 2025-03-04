CREATE OR REPLACE PACKAGE BODY td_utils
AS

   PROCEDURE dirpath_list( p_dirpath IN VARCHAR2 )
   AS
      LANGUAGE JAVA
      NAME 'TdUtils.getDirList( java.lang.String )';

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE directory_list( p_directory IN VARCHAR2 )   
   AS
      o_ev       evolve_ot := evolve_ot( p_module => 'td_utils.directory_list' );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT evolve.is_debugmode
      THEN
	 dirpath_list( get_dir_path( p_directory ) );
      END IF;

      o_ev.clear_app_info;
   END directory_list;

   -- procedure executes the host_cmd function and raises an exception with the return code
   PROCEDURE host_cmd( p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ' )
   AS
      l_retval   NUMBER;
      o_ev       evolve_ot := evolve_ot( p_module => 'td_utils.host_cmd' );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT evolve.is_debugmode
      THEN
         l_retval := host_cmd( p_cmd, p_stdin );

         IF l_retval <> 0
         THEN
            evolve.raise_err( 'host_cmd' );
         END IF;
      END IF;

      evolve.log_msg( 'Host command: ' || p_cmd, 3 );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END host_cmd;

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE check_duplicate ( 
      p_source_directory VARCHAR2, 
      p_source_filename VARCHAR2, 
      p_directory VARCHAR2, 
      p_filename VARCHAR2
   )
   AS
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;
      l_source_file VARCHAR2(61) := upper( p_source_directory ) || ':'|| p_source_filename;
      
      l_duplicate BOOLEAN  := FALSE;
      o_ev       evolve_ot := evolve_ot( p_module => 'td_utils.check_duplicate' );
   BEGIN

      evolve.log_msg( 'Source file: '|| l_source_file, 5 );
      evolve.log_msg( 'File: '|| l_file, 5 );

      -- if the copy process is just a duplicate process, then raise and exception
      IF lower( p_source_directory ) = lower( p_directory )
	 AND lower( p_source_filename ) = lower( p_filename )
      THEN
	 evolve.log_msg( 'DUPLICATE_FILE exception raised', 4 );
	 l_duplicate := TRUE;
	 RAISE duplicate_file;
      END IF;
      
      evolve.log_msg( 'No duplicate files detected', 4 );

      o_ev.clear_app_info;
   
   END check_duplicate;

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE copy_file( 
      p_source_directory VARCHAR2, 
      p_source_filename VARCHAR2, 
      p_directory VARCHAR2, 
      p_filename VARCHAR2
   )
   AS
      l_src_fh	 utl_file.file_type;
      l_dest_fh  utl_file.file_type;
      l_buf 	 RAW(32000);
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;
      l_source_file VARCHAR2(61) := upper( p_source_directory ) || ':'|| p_source_filename;

      o_ev       evolve_ot := evolve_ot( p_module => 'td_utils.copy_file' );
   BEGIN
      
      evolve.log_msg('Source file: ' || l_source_file, 5 );
      evolve.log_msg('Destination file: ' || l_file, 5 );
      
      l_src_fh	 := utl_file.fopen( p_source_directory, p_source_filename,'rb');
      l_dest_fh  := utl_file.fopen( p_directory, p_filename, 'wb');

      -- if the copy process is just a duplicate process, then raise and exception
      check_duplicate( p_source_directory => p_source_directory,
      		       p_source_filename  => p_source_filename,
		       p_directory	  => p_directory,
		       p_filename	  => p_filename );

      IF NOT evolve.is_debugmode
      THEN

	 BEGIN
	    o_ev.change_action( 'copy file' );
	    LOOP
	       utl_file.get_raw(l_src_fh,l_buf,32000);
	       utl_file.put_raw(l_dest_fh,l_buf,TRUE); -- AND flush
	    END LOOP;
	
	 EXCEPTION
	    WHEN no_data_found 
	    THEN
	       -- this is not really an exception
	       -- just done with all the data
	       o_ev.clear_app_info;
	    WHEN others 
	    THEN
	       -- make sure we close the previous file handles
	       utl_file.fclose(l_src_fh);
	       utl_file.fclose(l_dest_fh);
               o_ev.clear_app_info;
	       RAISE;
	 END; 
      END IF;

      evolve.log_msg( 'File ' || l_source_file || ' copied to ' || l_file, 3 );
      
      o_ev.clear_app_info;
   
   END copy_file;
   
   -- procedure executes the move_file function and raises an exception with the return code
   PROCEDURE move_file( 
      p_source_directory VARCHAR2, 
      p_source_filename  VARCHAR2, 
      p_directory        VARCHAR2, 
      p_filename         VARCHAR2
   )
   AS
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;
      l_source_file VARCHAR2(61) := upper( p_source_directory ) || ':'|| p_source_filename;
      e_diff_fs  EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_diff_fs, -29292);

      o_ev       evolve_ot := evolve_ot( p_module => 'td_utils.move_file' );
   BEGIN
      
      evolve.log_msg('Source file: ' || l_source_file, 5 );
      evolve.log_msg('Destination file: ' || l_file, 5 );

      -- if the copy process is just a duplicate process, then raise and exception
      check_duplicate( p_source_directory => p_source_directory,
      		       p_source_filename  => p_source_filename,
		       p_directory	  => p_directory,
		       p_filename	  => p_filename );


      IF NOT evolve.is_debugmode
      THEN

	 BEGIN
	    utl_file.frename( p_source_directory, p_source_filename, p_directory, p_filename, TRUE );
	 EXCEPTION
	    WHEN e_diff_fs
	    THEN
	       RAISE different_filesystems;
               o_ev.clear_app_info;
	 END;
      END IF;

      evolve.log_msg( 'File ' || l_source_file||' moved to ' || l_file, 3 );

      o_ev.clear_app_info;
   
   END move_file;
   

   -- uses UTL_FILE to remove an OS level file
   PROCEDURE delete_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;

      o_ev         evolve_ot       := evolve_ot( p_module => 'td_utils.delete_file' );
   BEGIN

      IF NOT evolve.is_debugmode
      THEN
         UTL_FILE.fremove( p_directory, p_filename );
      END IF;
      
      evolve.log_msg( 'File ' || l_file || ' deleted', 3 );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         evolve.log_msg( 'File ' || l_file || ' could not be deleted, or does not exist', 3 );
         o_ev.clear_app_info;
   END delete_file;

   -- uses UTL_FILE to "touch" a file
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_fh        UTL_FILE.file_type;
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;

      o_ev        evolve_ot          := evolve_ot( p_module => 'td_utils.create_file' );
   BEGIN

      IF NOT evolve.is_debugmode
      THEN
         l_fh := UTL_FILE.fopen( p_directory, p_filename, 'W' );
         UTL_FILE.fclose( l_fh );
      END IF;
      
      evolve.log_msg( 'Empty file ' || l_file || ' created' );

      o_ev.clear_app_info;
   EXCEPTION
      WHEN others
      THEN
	 UTL_FILE.fclose( l_fh );
         o_ev.clear_app_info;
	 RAISE;
   END create_file;

   -- get the number of lines in a file
   FUNCTION get_numlines( p_directory IN VARCHAR2, p_filename IN VARCHAR2 )
      RETURN NUMBER
   AS
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2( 2000 );
      l_cnt    NUMBER             := 0;
      o_ev     evolve_ot          := evolve_ot( p_module => 'td_utils.get_numlines' );
   BEGIN
      
      evolve.log_msg( 'Getting number of lines for: '|| l_file, 4 );
      IF evolve.is_debugmode
      THEN
         evolve.log_msg( 'Returning 0 because of DEBUG mode' );
         o_ev.clear_app_info;
         RETURN 0;
      ELSE
         BEGIN
            l_fh := UTL_FILE.fopen( p_directory, p_filename, 'R', 32767 );

            LOOP
               UTL_FILE.get_line( l_fh, l_line );
               l_cnt := l_cnt + 1;
            END LOOP;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               UTL_FILE.fclose( l_fh );
               evolve.log_msg( 'Number of lines returned: '||l_cnt, 4 );
               o_ev.clear_app_info;
               RETURN l_cnt;
         END;
      END IF;
   END get_numlines;


   -- get the number of lines in a file
   FUNCTION get_command( p_name IN VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_command  VARCHAR2(400);
      o_ev       evolve_ot          := evolve_ot( p_module => 'td_utils.get_command' );
   BEGIN
      
      BEGIN
         SELECT regexp_replace( path || CASE WHEN path IS NULL THEN NULL ELSE '/' end || value || CASE WHEN flags IS NULL THEN NULL ELSE ' ' end || flags, '//','/' )
           INTO l_command
           FROM command_conf
          WHERE lower( name ) = lower( p_name);
      EXCEPTION
         WHEN no_data_found
            THEN evolve.raise_err( 'invalid_command' );
      END;
            
      evolve.log_msg( 'The command: ' || l_command, 5 );      
      
      o_ev.clear_app_info;
      RETURN l_command;
   END get_command;

   -- a procedure used to expand a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- returns several out parameters, including expected filename, filesize, and blocksize
   PROCEDURE expand_file( 
      p_directory   VARCHAR2, 
      p_filename    VARCHAR2,
      p_method      VARCHAR2
   )
   AS
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;

      l_file_exists BOOLEAN;
      -- construct the filename minus the very last extension to the file
      l_filebase       VARCHAR2( 100 ) := REGEXP_REPLACE( p_filename, '(.+)(\.)([^\.]+$)', '\1', 1, 0, 'i' );

      -- construct the file suffix
      l_file_ext       VARCHAR2( 20 )  := REGEXP_REPLACE( p_filename, '(.+)(\.)([^\.]+$)', '\3', 1, 0, 'i' );

      -- construct the absolute path of the file
      l_filepath       VARCHAR2( 200 ) := get_dir_path( p_directory ) || '/' || p_filename;

      -- construct the absolute path of the file minus the extension
      l_filebasepath   VARCHAR2( 200 ) := get_dir_path( p_directory ) || '/' || l_filebase;
      o_ev             evolve_ot       := evolve_ot( p_module => 'td_utils.expand_file' );
   BEGIN
      evolve.log_msg('Destination file: ' || l_file, 5 );
      
      -- now figure out what to do with each method
      CASE p_method
         WHEN gzip_method
         THEN
            host_cmd( get_command( p_name=> 'gunzip' )||' ' || l_filepath );
            evolve.log_msg( l_filepath || ' gunzipped', 3 );
         WHEN compress_method
         THEN
            host_cmd( get_command( p_name=> 'uncompress' ) || ' ' || l_filepath );
            evolve.log_msg( l_filepath || ' uncompressed', 3 );
         WHEN bzip2_method
         THEN
            host_cmd( get_command( p_name=>'bunzip2' ) || ' ' || l_filepath );
            evolve.log_msg( l_filepath || ' bunzipped', 3 );
         WHEN zip_method
         THEN
            host_cmd( get_command( p_name=>'unzip' ) || ' ' || l_filepath );
            evolve.log_msg( l_filepath || ' unzipped', 3 );
         ELSE
	    -- we did not recognize the method
	    evolve.raise_err( 'invalid_compress_method', p_method);
         END CASE;

      o_ev.clear_app_info;
   END expand_file;
   
   -- a procedure used to decrypt a file regardless of which method was used to encrypt it
   -- currently contains functionality for the following encryption methods: gpg
   -- returns several out parameters, including expected filename, filesize, and blocksize
   PROCEDURE decrypt_file( 
      p_directory      VARCHAR2, 
      p_filename       VARCHAR2,
      p_method         VARCHAR2,
      p_passphrase     VARCHAR2 DEFAULT NULL
   )
   AS
      l_file VARCHAR2(61)        := upper( p_directory ) || ':'|| p_filename;

      l_file_exists BOOLEAN;
      -- construct the filename minus the very last extension to the file
      l_filebase       VARCHAR2( 100 ) := REGEXP_REPLACE( p_filename, '(.+)(\.)([^\.]+$)', '\1', 1, 0, 'i' );

      -- construct the file suffix
      l_file_ext       VARCHAR2( 20 )  := REGEXP_REPLACE( p_filename, '(.+)(\.)([^\.]+$)', '\3', 1, 0, 'i' );

      -- construct the absolute path of the file
      l_filepath       VARCHAR2( 200 ) := get_dir_path( p_directory ) || '/' || p_filename;

      -- construct the absolute path of the file minus the extension
      l_filebasepath   VARCHAR2( 200 ) := get_dir_path( p_directory ) || '/' || l_filebase;

      o_ev             evolve_ot       := evolve_ot( p_module => 'td_utils.decrypt_file' );
   BEGIN
      evolve.log_msg('Destination file: ' || l_file, 5 );
      
      -- now figure out what to do with each method
      CASE p_method
         WHEN gpg_method
         THEN
            host_cmd( get_command( p_name => 'gpg_decrypt' ) || ' ' || l_filepath || ' '
                      || l_filebasepath,
                      p_passphrase
                    );
            evolve.log_msg( l_filepath || ' decrypted', 3 );
         ELSE
	    -- we did not recognize the file extension
	    evolve.raise_err( 'invalid_encrypt_method', p_method);
      END CASE;

      o_ev.clear_app_info;
   END decrypt_file;

   -- checks things about a table depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_table
   (
      p_owner         VARCHAR2,
      p_table         VARCHAR2,
      p_dblink        VARCHAR2 DEFAULT NULL,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_iot           VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL,
      p_external      VARCHAR2 DEFAULT NULL
   )
   AS
      l_tab_name         VARCHAR2( 61 )        := UPPER( p_owner ) || '.' || UPPER( p_table );
      l_part_name        VARCHAR2( 92 )        := l_tab_name || ':' || UPPER( p_partname );
      l_partitioned      VARCHAR2( 3 );
      l_part_type        VARCHAR2( 10 );
      l_iot              VARCHAR2( 3 );
      l_compressed       VARCHAR2( 3 );
      l_partition_name   all_tab_partitions.partition_name%TYPE;
      l_sql              VARCHAR2(4000);
   BEGIN

      -- if this is a partitioned table
      IF ( p_partname IS NOT NULL OR td_core.is_true( p_partitioned, true ))
         -- find out if it's partitioned or subpartitioned
      THEN
         
         l_part_type := get_tab_part_type( p_owner, p_table, p_partname, p_dblink );
         
         evolve.log_variable( 'l_part_type', l_part_type );
         
      END IF;
         
      -- now get compression, partitioning and iot information
      l_sql := 
         q'{SELECT CASE WHEN compression = 'DISABLED' THEN 'no' WHEN compression = 'N/A' THEN 'no' WHEN compression IS NULL THEN 'no' ELSE 'yes' END, }' 
      || q'{LOWER( partitioned ) partitioned, CASE iot_type WHEN 'IOT' THEN 'yes' ELSE 'no' END iot }'
      || q'{FROM all_tables}'
      || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END 
      || q'{ WHERE owner = UPPER( '}'
      || p_owner
      || q'{' ) }' 
      || q'{AND table_name = UPPER( '}'
      || p_table
      || q'{' )}';
         
      evolve.log_variable( 'l_sql',l_sql);

      BEGIN
         EXECUTE IMMEDIATE l_sql
         INTO l_compressed,
              l_partitioned,
              l_iot;
      EXCEPTION 
         WHEN no_data_found
            THEN
            evolve.raise_err( 'no_tab', l_tab_name );
      END;

      evolve.log_variable( 'l_compressed',l_compressed);
      evolve.log_variable( 'l_partitioned',l_partitioned);
      evolve.log_variable( 'l_iot',l_iot);
      
      IF l_partitioned = 'yes' AND p_partname IS NULL AND p_compressed IS NOT NULL
      THEN
         evolve.raise_err( 'parms_not_compatible',
                               'P_COMPRESSED requires P_PARTNAME when the table is partitioned' );
      END IF;

      IF p_partname IS NOT NULL
      THEN
         IF l_partitioned = 'no'
         THEN
            evolve.raise_err( 'not_partitioned', l_tab_name );
         END IF;
         
         IF l_part_type = 'part'
         THEN
            
            l_sql := 
               q'{SELECT CASE WHEN compression = 'DISABLED' THEN 'no' WHEN compression = 'N/A' THEN 'no' WHEN compression IS NULL THEN 'no' ELSE 'yes' END }'
            || q'{ FROM all_tab_partitions}'
            || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
            || q'{ WHERE table_owner = UPPER( '}'
            || p_owner 
            || q'{' ) AND table_name = UPPER( '}'
            || p_table 
            || q'{' ) AND partition_name = UPPER( '}'
            || p_partname 
            || q'{' )}';

            evolve.log_variable( 'l_sql', l_sql );

            BEGIN

               EXECUTE IMMEDIATE l_sql
               INTO l_compressed;
            EXCEPTION
               WHEN NO_DATA_FOUND
                  THEN
                  evolve.raise_err( 'no_part', l_part_name );
            END;
          
            evolve.log_variable( 'l_compressed', l_compressed );
            
         ELSIF l_part_type = 'subpart'
         THEN
            
            l_sql :=
               q'{SELECT CASE WHEN compression = 'DISABLED' THEN 'no' WHEN compression = 'N/A' THEN 'no' WHEN compression IS NULL THEN 'no' ELSE 'yes' END }'
            || q'{ FROM all_tab_subpartitions}'
            || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
            || q'{ WHERE table_owner = UPPER( '}'
            || p_owner 
            || q'{' ) AND table_name = UPPER( '}'
            || p_table 
            || q'{' ) AND subpartition_name = UPPER( '}'
            || p_partname 
            || q'{' )}';
            
            evolve.log_variable( 'l_sql', l_sql );

           BEGIN
         
               EXECUTE IMMEDIATE l_sql
               INTO l_compressed;
     
            EXCEPTION
               WHEN NO_DATA_FOUND
                  THEN
                  evolve.raise_err( 'no_part', l_part_name );
             END;
            
          END IF;
               
       END IF;
       

      CASE
         WHEN td_core.is_true( p_partitioned, TRUE ) AND NOT td_core.is_true( l_partitioned )
         THEN
            evolve.raise_err( 'not_partitioned', l_tab_name );
         WHEN NOT td_core.is_true( p_partitioned, TRUE ) AND td_core.is_true( l_partitioned )
         THEN
            evolve.raise_err( 'partitioned', l_tab_name );
         WHEN td_core.is_true( p_external, TRUE ) AND NOT ext_table_exists( p_owner => p_owner, p_table => p_table )
         THEN
            evolve.raise_err( 'not_external', l_tab_name );
         WHEN NOT td_core.is_true( p_external, TRUE ) AND ext_table_exists( p_owner => p_owner, p_table => p_table )
         THEN
            evolve.raise_err( 'external', l_tab_name );
         WHEN td_core.is_true( p_iot, TRUE ) AND NOT td_core.is_true( l_iot )
         THEN
            evolve.raise_err( 'not_iot', l_tab_name );
         WHEN NOT td_core.is_true( p_iot, TRUE ) AND td_core.is_true( l_iot )
         THEN
            evolve.raise_err( 'iot', l_tab_name );
         WHEN td_core.is_true( p_compressed, TRUE ) AND NOT td_core.is_true( l_compressed )
         THEN
            evolve.raise_err( 'not_compressed', CASE
                                     WHEN p_partname IS NULL
                                        THEN l_tab_name
                                     ELSE l_part_name
                                  END );
         WHEN NOT td_core.is_true( p_compressed, TRUE ) AND td_core.is_true( l_compressed )
         THEN
            evolve.raise_err( 'compressed', CASE
                                     WHEN p_partname IS NULL
                                        THEN l_tab_name
                                     ELSE l_part_name
                                  END );
         ELSE
            NULL;
      END CASE;
   END check_table;
   
   -- checks things about an index depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_index(
      p_owner         VARCHAR2,
      p_index         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_index_type    VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL,
      p_unique        VARCHAR2 DEFAULT NULL
   )
   AS
      l_ind_name         VARCHAR2( 61 )        := UPPER( p_owner ) || '.' || UPPER( p_index );
      l_part_name        VARCHAR2( 92 )        := l_ind_name || ':' || UPPER( p_partname );
      l_partitioned      VARCHAR2( 3 );
      l_compressed       VARCHAR2( 3 );
      l_unique           VARCHAR2( 3 );
      l_partition_name   all_tab_partitions.partition_name%TYPE;
      l_index_type       all_indexes.index_type%TYPE;
   BEGIN
      -- now get compression, partitioning and iot information
      BEGIN
         SELECT CASE
                   WHEN compression = 'DISABLED'
                      THEN 'no'
                   WHEN compression = 'N/A'
                      THEN 'no'
                   WHEN compression IS NULL
                      THEN 'no'
                   ELSE 'yes'
                END,
                LOWER( partitioned ) partitioned,
                lower( index_type) index_type, 
                CASE uniqueness
                   WHEN 'UNIQUE'
                      THEN 'yes'
                   ELSE 'no'
                END UNIQUENESS
           INTO l_compressed,
                l_partitioned, 
                l_index_type,
                l_unique
           FROM all_indexes
          WHERE owner = UPPER( p_owner ) AND index_name = UPPER( p_index );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_ind', l_ind_name );
      END;

      -- now just work through the gathered information and raise the appropriate exceptions.
      IF l_partitioned = 'yes' AND p_partname IS NULL AND p_compressed IS NOT NULL
      THEN
         evolve.raise_err( 'parms_not_compatible',
                               'P_COMPRESSED requires P_PARTNAME when the index is partitioned' );
      END IF;

      IF p_partname IS NOT NULL
      THEN
         IF l_partitioned = 'no'
         THEN
            evolve.raise_err( 'not_partitioned', l_ind_name );
         END IF;

         BEGIN
            SELECT CASE
                      WHEN compression = 'DISABLED'
                         THEN 'no'
                      WHEN compression = 'N/A'
                         THEN 'no'
                      WHEN compression IS NULL
                         THEN 'no'
                      ELSE 'yes'
                   END
              INTO l_compressed
              FROM all_ind_partitions
             WHERE index_owner = UPPER( p_owner )
               AND index_name = UPPER( p_index )
               AND partition_name = UPPER( p_partname );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               evolve.raise_err( 'no_part', l_part_name );
         END;
      END IF;

      CASE
         WHEN td_core.is_true( p_partitioned, TRUE ) AND NOT td_core.is_true( l_partitioned )
         THEN
            evolve.raise_err( 'not_partitioned', l_ind_name );
         WHEN NOT td_core.is_true( p_partitioned, TRUE ) AND td_core.is_true( l_partitioned )
         THEN
            evolve.raise_err( 'partitioned', l_ind_name );
         WHEN td_core.is_true( p_compressed, TRUE ) AND NOT td_core.is_true( l_compressed )
         THEN
            evolve.raise_err( 'not_compressed', CASE
                                     WHEN p_partname IS NULL
                                        THEN l_ind_name
                                     ELSE l_part_name
                                  END );
         WHEN NOT td_core.is_true( p_compressed, TRUE ) AND td_core.is_true( l_compressed )
         THEN
            evolve.raise_err( 'compressed', CASE
                                     WHEN p_partname IS NULL
                                        THEN l_ind_name
                                     ELSE l_part_name
                                  END );
         ELSE
            NULL;
      END CASE;
   END check_index;

   -- checks to see if a particular column is part of a table
   -- raises an exception if the specified things are not true
   PROCEDURE check_column( p_owner VARCHAR2, p_table VARCHAR2, p_column VARCHAR2, p_data_type VARCHAR2 DEFAULT NULL )
   AS
      l_tab_name      VARCHAR2( 61 )                     := UPPER( p_owner ) || '.' || UPPER( p_table );
      l_column_name   all_tab_columns.column_name%TYPE;
   BEGIN
      SELECT DISTINCT column_name
                 INTO l_column_name
                 FROM all_tab_columns
                WHERE owner = UPPER( p_owner )
                  AND table_name = UPPER( p_table )
                  AND column_name = UPPER( p_column )
                  AND REGEXP_LIKE( data_type, NVL( p_data_type, '.' ), 'i' );
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         evolve.raise_err( 'no_column',
                                  UPPER( p_column )
                               || CASE
                                     WHEN p_data_type IS NULL
                                        THEN NULL
                                     ELSE ' of data type ' || UPPER( p_data_type )
                                  END
                               || ' for table '
                               || l_tab_name
                             );
      WHEN TOO_MANY_ROWS
      THEN
         evolve.raise_err( 'too_many_objects' );
   END check_column;

   -- checks things about an object depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_object
      ( 
        p_owner        VARCHAR2, 
        p_object       VARCHAR2,
        p_object_type  VARCHAR2 DEFAULT NULL,
        p_dblink       VARCHAR2 DEFAULT NULL  
      )
   AS
      l_obj_name      VARCHAR2( 61 )                 := UPPER( p_owner ) || '.' || UPPER( p_object );
      l_object_name   all_objects.object_name%TYPE;
      l_sql           VARCHAR2(4000);
   BEGIN
      
      l_sql :=
         q'{SELECT DISTINCT object_name FROM all_objects}'
      || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
      || q'{ WHERE owner = UPPER( '}'
      || p_owner
      || q'{' ) AND object_name = UPPER( '}'
      || p_object
      || q'{' ) AND REGEXP_LIKE( object_type, NVL( '}'
      || p_object_type
      || q'{', '.' ), 'i' )}';

      evolve.log_variable( 'l_sql', l_sql );
            
      BEGIN
         SELECT DISTINCT object_name
           INTO l_object_name
           FROM all_objects                
          WHERE owner = UPPER( p_owner )
            AND object_name = UPPER( p_object )
            AND REGEXP_LIKE( object_type, NVL( p_object_type, '.' ), 'i' );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_or_wrong_object', l_obj_name );
         WHEN TOO_MANY_ROWS
         THEN
            evolve.raise_err( 'too_many_objects' );
      END;
   END check_object;

   -- used to get the path associated with a directory location
   FUNCTION get_dir_path( p_directory VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER( p_directory );

      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         evolve.raise_err( 'no_dir_obj', p_directory );
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_dirname   all_directories.directory_name%TYPE;
   BEGIN
      SELECT directory_name
        INTO l_dirname
        FROM all_directories
       WHERE directory_path = p_dir_path;

      RETURN l_dirname;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         evolve.raise_err( 'no_dir_path', p_dir_path );
      WHEN TOO_MANY_ROWS
      THEN
         evolve.raise_err( 'too_many_dirs', p_dir_path );
   END get_dir_name;

   -- returns a boolean
   -- does a check to see if a table exists
   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_table   all_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM all_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END table_exists;
   
   
   -- returns a boolean
   -- does a check to see if a index exists
   FUNCTION index_exists( p_owner VARCHAR2, p_index VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_index   all_indexes.index_name%TYPE;
   BEGIN
      SELECT index_name
        INTO l_index
        FROM all_indexes
       WHERE owner = UPPER( p_owner ) AND index_name = UPPER( p_index );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END index_exists;

   -- returns a boolean
   -- does a check to see if an external table exists
   FUNCTION ext_table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_table   all_external_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM all_external_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END ext_table_exists;

   -- returns a boolean
   -- does a check to see if table is partitioned
   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_partitioned   all_tables.partitioned%TYPE;
   BEGIN
      IF NOT table_exists( UPPER( p_owner ), UPPER( p_table ))
      THEN
         evolve.raise_err( 'no_tab', p_owner || '.' || p_table );
      END IF;

      SELECT partitioned
        INTO l_partitioned
        FROM all_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      CASE
         WHEN td_core.is_true( l_partitioned )
         THEN
            RETURN TRUE;
         WHEN NOT td_core.is_true( l_partitioned )
         THEN
            RETURN FALSE;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END is_part_table;
   
   -- returns a boolean
   -- does a check to see if index is partitioned
   FUNCTION is_part_index( p_owner VARCHAR2, p_index VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_partitioned   all_indexes.partitioned%TYPE;
   BEGIN
      IF NOT index_exists( UPPER( p_owner ), UPPER( p_index ))
      THEN
         evolve.raise_err( 'no_tab', p_owner || '.' || p_index );
      END IF;

      SELECT partitioned
        INTO l_partitioned
        FROM all_indexes
       WHERE owner = UPPER( p_owner ) AND index_name = UPPER( p_index );

      CASE
         WHEN td_core.is_true( l_partitioned )
         THEN
            RETURN TRUE;
         WHEN NOT td_core.is_true( l_partitioned )
         THEN
            RETURN FALSE;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END is_part_index;

   -- returns a boolean
   -- does a check to see if table is index-organized
   FUNCTION is_iot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_iot   all_tables.iot_type%TYPE;
   BEGIN
      IF NOT table_exists( UPPER( p_owner ), UPPER( p_table ))
      THEN
         evolve.raise_err( 'no_tab', p_owner || '.' || p_table );
      END IF;

      SELECT iot_type
        INTO l_iot
        FROM all_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      CASE l_iot
         WHEN 'IOT'
         THEN
            RETURN TRUE;
         ELSE
            RETURN FALSE;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END is_iot;

   -- returns a boolean
   -- does a check to see if a object exists
   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_object   all_objects.object_name%TYPE;
   BEGIN
      SELECT DISTINCT object_name
                 INTO l_object
                 FROM all_objects
                WHERE owner = UPPER( p_owner ) AND object_name = UPPER( p_object );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END object_exists;

   -- returns 'part' or 'subpart' depending on the partition type
   FUNCTION get_tab_part_type
      ( 
        p_owner     VARCHAR2, 
        p_table     VARCHAR2,
        p_partname  VARCHAR2 DEFAULT NULL, 
        p_dblink    VARCHAR2 DEFAULT NULL
      )
      RETURN VARCHAR2
   AS
      l_num        NUMBER;
      l_sql        VARCHAR2(4000);
      l_obj_type   all_objects.object_type%TYPE;
   BEGIN
      
      IF p_partname IS NULL
      THEN
         
         l_sql :=
            q'{SELECT count(*) FROM all_tab_subpartitions}'
         || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
         || q'{ WHERE table_owner = UPPER( '}'
         || p_owner
         || q'{' ) AND table_name = UPPER( '}'
         || p_table
         || q'{' )}';

         evolve.log_variable( 'l_sql', l_sql );

         EXECUTE IMMEDIATE l_sql
         INTO l_num;

         evolve.log_variable( 'l_num', l_num );

         IF l_num > 0
         THEN
            RETURN 'subpart';
         END IF;

         l_sql :=
            q'{SELECT count(*) FROM all_tab_partitions}'
         || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
         || q'{ WHERE table_owner = UPPER( '}'
         || p_owner
         || q'{' ) AND table_name = UPPER( '}'
         || p_table
         || q'{' )}';

         evolve.log_variable( 'l_sql', l_sql );

         EXECUTE IMMEDIATE l_sql
         INTO l_num;
                  
         IF l_num > 0
         THEN
            RETURN 'part';
         ELSE
            RETURN 'normal';
         END IF;
         
      ELSE

         l_sql :=
               q'{SELECT CASE WHEN object_type LIKE '% SUBPARTITION' THEN 'subpart' WHEN object_type LIKE '% PARTITION' THEN 'part' ELSE 'normal' END object_type }'
            || q'{ FROM all_objects}'
            || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
            || q'{ WHERE owner = UPPER( '}'
            || p_owner
            || q'{' ) AND object_name = UPPER( '}'
            || p_table
            || q'{' ) AND subobject_name = UPPER( '}'
            || p_partname
            || q'{' )}';

        evolve.log_variable( 'l_sql', l_sql );
               
         BEGIN

            EXECUTE IMMEDIATE l_sql
            INTO l_obj_type;

            RETURN l_obj_type;
         
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               RETURN NULL;
         END;

       END IF;

   END get_tab_part_type;

   -- returns the partition name for a given subpartition name
   FUNCTION get_part_for_subpart
      ( p_owner VARCHAR2, 
        p_segment VARCHAR2, 
        p_subpart VARCHAR2, 
        p_segment_type VARCHAR2 
      )
      RETURN VARCHAR2
   AS
      l_part   all_ind_subpartitions.partition_name%TYPE;
   BEGIN

      IF lower( p_segment_type ) = 'index'
      THEN

         SELECT partition_name
           INTO l_part
           FROM all_ind_subpartitions
          WHERE index_owner = UPPER( p_owner ) AND index_name = UPPER( p_segment )
            AND subpartition_name = UPPER( p_subpart );
         
      ELSIF lower( p_segment_type ) = 'table'
      THEN
         
         SELECT partition_name
           INTO l_part
           FROM all_tab_subpartitions
          WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_segment )
            AND subpartition_name = UPPER( p_subpart );
         
      ELSE 
         evolve.raise_err('seg_not_supported',p_segment_type);
      END IF;

      RETURN l_part;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END get_part_for_subpart;
   
   -- returns an INTERSECT between the columns in two different tables
   -- used to ensure all columns from both tables are in a list
   FUNCTION get_column_list
      ( 
        p_owner         VARCHAR2, 
        p_table         VARCHAR2, 
        p_source_owner  VARCHAR2 DEFAULT NULL, 
        p_source_table  VARCHAR2 DEFAULT NULL,
        p_set_oper      VARCHAR2 DEFAULT 'intersect',
        p_dblink        VARCHAR2 DEFAULT NULL 
      )
      RETURN VARCHAR2
   AS
      l_sql         VARCHAR2(4000);
      l_collist     VARCHAR2(4000);
   BEGIN
      
      -- find the least-restrictive set of columns in common
      -- supports a single table or two tables
      -- also supports the inclusive of a db_link
      l_sql :=    q'{SELECT listagg( column_name, ', ') }'
               || q'{within GROUP ( ORDER BY column_id ) }'
               || chr(10)
               || 'FROM ('
               || chr(10)
               || 'SELECT column_name, column_id '
               || chr(10)
               || 'FROM all_tab_columns'
               || CASE WHEN p_dblink IS NOT NULL AND p_source_owner IS null THEN '@'||p_dblink ELSE NULL END
               || chr(10)
               || 'WHERE table_name = '''
               || upper( p_table ) 
               || ''''
               || chr(10)
               || 'AND owner = '''
               || upper( p_owner ) 
               || ''''
               || chr(10)
               || CASE WHEN p_source_owner IS NULL THEN NULL ELSE
                  p_set_oper
               || chr(10)
               || 'SELECT column_name, column_id '
               || chr(10)
               || 'FROM all_tab_columns'
               || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
               || chr(10)
               || 'WHERE table_name = '''
               || upper( p_source_table ) 
               || ''''
               || chr(10)
               || 'AND owner = '''
               || upper( p_source_owner ) 
               || '''' END
               || ' )';
               
     evolve.log_variable( 'l_sql', l_sql );         
         
     EXECUTE IMMEDIATE l_sql
               INTO l_collist;
               
     l_collist := lower( l_collist );

      RETURN l_collist;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END get_column_list;
   
   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_directory   VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT NULL,
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   AS
      l_output        UTL_FILE.file_type;
      l_thecursor     INTEGER            DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2( 2000 );
      l_status        INTEGER;
      l_colcnt        NUMBER             DEFAULT 0;
      l_delimiter     VARCHAR2( 5 )      DEFAULT '';
      l_cnt           NUMBER             DEFAULT 0;
      l_mode          VARCHAR2( 1 )      := CASE LOWER( p_append )
         WHEN 'yes'
            THEN 'a'
         ELSE 'w'
      END;
      l_exists        BOOLEAN;
      l_length        NUMBER;
      l_blocksize     NUMBER;
      e_no_var        EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_var, -1007 );
      o_ev            evolve_ot          := evolve_ot( p_module => 'td_utils.extract_query' );
   BEGIN
      l_output := UTL_FILE.fopen( p_directory, p_filename, l_mode, 32767 );
      DBMS_SQL.parse( l_thecursor, p_query, DBMS_SQL.native );
      o_ev.change_action( 'define columns' );

      FOR i IN 1 .. 255
      LOOP
         BEGIN
            DBMS_SQL.define_column( l_thecursor, i, l_columnvalue, 2000 );
            l_colcnt := i;
         EXCEPTION
            WHEN e_no_var
            THEN
               o_ev.clear_app_info;
               EXIT;
         END;
      END LOOP;

      DBMS_SQL.define_column( l_thecursor, 1, l_columnvalue, 2000 );
      l_status := DBMS_SQL.EXECUTE( l_thecursor );
      o_ev.change_action( 'extract records' );

      LOOP
         EXIT WHEN( DBMS_SQL.fetch_rows( l_thecursor ) <= 0 );
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE( l_thecursor, i, l_columnvalue );

            IF NOT evolve.is_debugmode
            THEN
               UTL_FILE.put( l_output, l_delimiter || p_quotechar || l_columnvalue || p_quotechar );
            END IF;

            l_delimiter := p_delimiter;
         END LOOP;

         UTL_FILE.new_line( l_output );
         l_cnt := l_cnt + 1;
      END LOOP;

      o_ev.change_action( 'Close cursor and handles' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );
      o_ev.clear_app_info;
      RETURN l_cnt;
   END extract_query;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   FUNCTION extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_directory   VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT NULL,
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   IS
      l_cnt           NUMBER           := 0;
      l_head_sql      VARCHAR( 1000 );
      l_extract_sql   VARCHAR2( 1000 );
      o_ev            evolve_ot        := evolve_ot( p_module => 'td_utils.extract_object' );
   BEGIN
      -- check that the source object exists and is something we can select from
      check_object( p_owner => p_owner, p_object => p_object, p_object_type => 'table$|view' );
      l_head_sql :=
            'select listagg(column_name,'''
         || p_delimiter
         || ''') within group (order by column_id) from '
         || '(select '''
         || p_quotechar
         || '''||column_name||'''
         || p_quotechar
         || ''' as column_name, column_id'
         || ' from all_tab_cols '
         || 'where table_name='''
         || UPPER( p_object )
         || ''' and owner='''
         || UPPER( p_owner )
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;
      evolve.log_msg( 'Headers query: ' || l_head_sql, 3 );
      evolve.log_msg( 'Extract query: ' || l_extract_sql, 3 );

      IF NOT evolve.is_debugmode
      THEN
         IF td_core.is_true( p_headers )
         THEN
            o_ev.change_action( 'Extract headers to file' );
            l_cnt :=
               extract_query( p_query          => l_head_sql,
                              p_directory      => p_directory,
                              p_filename       => p_filename,
                              p_delimiter      => p_delimiter,
                              p_quotechar      => NULL,
                              p_append         => p_append
                            );
         END IF;

         o_ev.change_action( 'Extract data to file' );
         l_cnt :=
              l_cnt
            + extract_query( p_query          => l_extract_sql,
                             p_directory      => p_directory,
                             p_filename       => p_filename,
                             p_delimiter      => p_delimiter,
                             p_quotechar      => p_quotechar,
                             p_append         => CASE
                                WHEN td_core.is_true( p_headers )
                                   THEN 'yes'
                                ELSE p_append
                             END
                           );
      END IF;

      o_ev.clear_app_info;
      RETURN l_cnt;
   END extract_object;
   
   PROCEDURE log_msg
      ( 
        p_msg           log_table.action%type,
        p_level         log_table.logging_level%type DEFAULT 1,
        p_action        log_table.action%type        DEFAULT NULL,
        p_module        log_table.module%type        DEFAULT NULL,
        p_client_info   log_table.client_info%type   DEFAULT NULL
      )
   AS
      o_ev            evolve_ot  := evolve_ot( 
                                               p_module =>      NVL(p_module, td_inst.module),
                                               p_action =>      NVL(p_action, td_inst.action),
                                               p_client_info => NVL( p_client_info, td_inst.client_info)
                                             );
   BEGIN
      
      CASE
      WHEN p_level = -1
      THEN
      
         evolve.log_err( p_msg );
         
      ELSE 

         evolve.log_msg( p_msg, p_level );
         
      END CASE;

      o_ev.clear_app_info;

   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;

   END log_msg;
END td_utils;
/

SHOW errors