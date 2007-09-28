CREATE OR REPLACE PACKAGE BODY td_core
AS
   -- procedure executes the host_cmd function and raises an exception with the return code
   PROCEDURE host_cmd( p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ' )
   AS
      l_retval   NUMBER;
      o_td       tdtype := tdtype( p_module => 'host_cmd' );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT td_inst.is_debugmode
      THEN
         l_retval := host_cmd( p_cmd, p_stdin );

         IF l_retval <> 0
         THEN
            raise_application_error
                            ( -20020,
                              'Java Error: method hostCmd made unsuccessful system calls'
                            );
         END IF;
      END IF;

      td_inst.log_msg( 'Host command: ' || p_cmd, 3 );
      o_td.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END host_cmd;

   -- procedure executes the copy_file function and raises an exception with the return code
   PROCEDURE copy_file( p_srcfile VARCHAR2, p_dstfile VARCHAR2 )
   AS
      l_retval   NUMBER;
      o_td       tdtype := tdtype( p_module => 'copy_file' );
   BEGIN
      DBMS_JAVA.set_output( 1000000 );

      IF NOT td_inst.is_debugmode
      THEN
         l_retval := copy_file( p_srcfile, p_dstfile );

         IF l_retval <> 0
         THEN
            raise_application_error
                            ( -20020,
                                 'Java Error: method TdCore.copyFile was unable to copy '
                              || p_srcfile
                              || ' to '
                              || p_dstfile
                            );
         END IF;
      END IF;

      td_inst.log_msg( 'File ' || p_srcfile || ' copied to ' || p_dstfile, 3 );
      o_td.clear_app_info;
   END copy_file;

   -- uses UTL_FILE to remove an OS level file
   PROCEDURE delete_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_retval     NUMBER;
      l_filepath   VARCHAR2( 100 );
      o_td         tdtype          := tdtype( p_module => 'delete_file' );
   BEGIN
      l_filepath := td_sql.get_dir_path( p_directory ) || '/' || p_filename;

      IF NOT td_inst.is_debugmode
      THEN
         UTL_FILE.fremove( p_directory, p_filename );
      END IF;

      td_inst.log_msg( 'File ' || l_filepath || ' deleted', 3 );
      o_td.clear_app_info;
   EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         td_inst.log_msg( l_filepath || ' could not be deleted, or does not exist' );
   END delete_file;

   -- uses UTL_FILE to "touch" a file
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 )
   AS
      l_fh        UTL_FILE.file_type;
      l_dirpath   VARCHAR2( 100 );
      o_td        tdtype             := tdtype( p_module => 'create_file' );
   BEGIN
      l_dirpath := td_sql.get_dir_path( p_directory ) || '/' || p_filename;

      IF NOT td_inst.is_debugmode
      THEN
         l_fh := UTL_FILE.fopen( p_directory, p_filename, 'W' );
      END IF;

      td_inst.log_msg( 'File ' || l_dirpath || ' created', 3 );
      o_td.clear_app_info;
   END create_file;

   -- get the number of lines in a file
   FUNCTION get_numlines( p_dirname IN VARCHAR2, p_filename IN VARCHAR2 )
      RETURN NUMBER
   AS
      l_fh     UTL_FILE.file_type;
      l_line   VARCHAR2( 2000 );
      l_cnt    NUMBER             := 0;
      o_td     tdtype             := tdtype( p_module => 'get_numlines' );
   BEGIN
      IF td_inst.is_debugmode
      THEN
         td_inst.log_msg( td_inst.module || ' returning 0 because of DEBUG mode' );
         o_td.clear_app_info;
         RETURN 0;
      ELSE
         BEGIN
            l_fh := UTL_FILE.fopen( p_dirname, p_filename, 'R', 32767 );

            LOOP
               UTL_FILE.get_line( l_fh, l_line );
               l_cnt := l_cnt + 1;
            END LOOP;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               UTL_FILE.fclose( l_fh );
               o_td.clear_app_info;
               RETURN l_cnt;
         END;
      END IF;
   END get_numlines;

   -- a function used to unzip a file regardless of which library was used to zip it
   -- currently contains functionality for the following libraries: gzip, zip, compress, and bzip2
   -- function returns what the name should be after the unzip process
   FUNCTION unzip_file( p_dirpath VARCHAR2, p_filename VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_compressed     BOOLEAN         := TRUE;
      l_filebase       VARCHAR2( 50 );
      l_filesuf        VARCHAR2( 20 );
      l_filepath       VARCHAR2( 200 ) := p_dirpath || '/' || p_filename;
      l_filebasepath   VARCHAR2( 200 );
      l_cmd            VARCHAR2( 200 );
      l_return         VARCHAR2( 200 );
      l_file_exists    BOOLEAN;
      l_file_size      NUMBER;
      l_blocksize      NUMBER;
      o_td             tdtype          := tdtype( p_module => 'unzip_file' );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename, '\.[^\.]+$', NULL, 1, 1, 'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;
      td_inst.log_msg( l_filepath || ' checked for compression using standard libraries',
                       3
                     );

      CASE l_filesuf
         WHEN 'gz'
         THEN
            host_cmd( 'gzip -df ' || l_filepath );
            td_inst.log_msg( l_filepath || ' gunzipped', 3 );
         WHEN 'Z'
         THEN
            host_cmd( 'uncompress ' || l_filepath );
            td_inst.log_msg( l_filepath || ' uncompressed', 3 );
         WHEN 'bz2'
         THEN
            host_cmd( 'bunzip2 ' || l_filepath );
            td_inst.log_msg( l_filepath || ' bunzipped', 3 );
         WHEN 'zip'
         THEN
            host_cmd( 'unzip ' || l_filepath );
            td_inst.log_msg( l_filepath || ' unzipped', 3 );
         ELSE
            -- this is the only case where the file wasn't compressed
            l_compressed := FALSE;
      END CASE;

      -- return either the expected uncompressed filename
      -- or the provided filename if no unzip process was performed
      IF l_compressed
      THEN
         l_return := l_filebasepath;
      ELSE
         l_return := l_filepath;
      END IF;

      IF td_inst.is_debugmode
      THEN
         td_inst.log_msg( 'File returned by UNZIP_FILE: ' || l_return );
      ELSE
         o_td.change_action( 'Check for extracted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( td_sql.get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize
                          );

         IF NOT l_file_exists
         THEN
            raise_application_error( td_inst.get_err_cd( 'file_not_found' ),
                                     td_inst.get_err_msg( 'file_not_found' )
                                   );
         END IF;
      END IF;

      o_td.clear_app_info;
      RETURN l_return;
   END unzip_file;

   -- a function used to decrypt a file regardless of which method was used to encrypt it
   -- currently contains functionality for the following encryption methods: gpg
   -- function returns what the name should be after the decryption process
   FUNCTION decrypt_file( p_dirpath VARCHAR2, p_filename VARCHAR2, p_passphrase VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_encrypted      BOOLEAN         := TRUE;
      l_filebase       VARCHAR2( 50 );
      l_filesuf        VARCHAR2( 20 );
      l_filepath       VARCHAR2( 200 ) := p_dirpath || '/' || p_filename;
      l_filebasepath   VARCHAR2( 200 );
      l_cmd            VARCHAR2( 200 );
      l_return         VARCHAR2( 200 );
      l_file_exists    BOOLEAN;
      l_file_size      NUMBER;
      l_blocksize      NUMBER;
      o_td             tdtype          := tdtype( p_module => 'decrypt_file' );
   BEGIN
      l_filebase := REGEXP_REPLACE( p_filename, '\.[^\.]+$', NULL, 1, 1, 'i' );
      l_filesuf := REGEXP_SUBSTR( p_filename, '[^\.]+$' );
      l_filebasepath := p_dirpath || '/' || l_filebase;

      CASE l_filesuf
         WHEN 'gpg'
         THEN
            host_cmd(    'gpg --no-tty --passphrase-fd 0 --batch --decrypt --output '
                      || l_filepath
                      || ' '
                      || l_filebasepath,
                      p_passphrase
                    );
         ELSE
            -- this is the only case where the extension wasn't recognized
            l_encrypted := FALSE;
      END CASE;

      -- return either the expected decrypted filename
      -- or the provided filename if the decryption process was performed
      IF l_encrypted
      THEN
         l_return := l_filebasepath;
      ELSE
         l_return := l_filepath;
      END IF;

      IF td_inst.is_debugmode
      THEN
         td_inst.log_msg( 'File returned by DECRYPT_FILE: ' || l_return );
      ELSE
         o_td.change_action( 'Check for decrypted file' );
         -- check and make sure the unzip process worked
         -- do this by checking to see if the expected file exists
         UTL_FILE.fgetattr( td_sql.get_dir_name( p_dirpath ),
                            l_return,
                            l_file_exists,
                            l_file_size,
                            l_blocksize
                          );

         IF NOT l_file_exists
         THEN
            raise_application_error( -20020, 'Filename to return does not exist' );
         END IF;
      END IF;

      o_td.clear_app_info;
      RETURN l_return;
   END decrypt_file;

   -- modified FROM tom kyte's "dump_csv":
   -- 1. allow a quote CHARACTER
   -- 2. allow FOR a FILE TO be appended TO
   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
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
      o_td            tdtype             := tdtype( p_module => 'extract_query' );
   BEGIN
      l_output := UTL_FILE.fopen( p_dirname, p_filename, l_mode, 32767 );
      DBMS_SQL.parse( l_thecursor, p_query, DBMS_SQL.native );
      o_td.change_action( 'Open Cursor to define columns' );

      FOR i IN 1 .. 255
      LOOP
         BEGIN
            DBMS_SQL.define_column( l_thecursor, i, l_columnvalue, 2000 );
            l_colcnt := i;
         EXCEPTION
            WHEN e_no_var
            THEN
               EXIT;
         END;
      END LOOP;

      DBMS_SQL.define_column( l_thecursor, 1, l_columnvalue, 2000 );
      l_status := DBMS_SQL.EXECUTE( l_thecursor );
      o_td.change_action( 'Open Cursor to pull back records' );

      LOOP
         EXIT WHEN( DBMS_SQL.fetch_rows( l_thecursor ) <= 0 );
         l_delimiter := '';

         FOR i IN 1 .. l_colcnt
         LOOP
            DBMS_SQL.COLUMN_VALUE( l_thecursor, i, l_columnvalue );

            IF NOT td_inst.is_debugmode
            THEN
               UTL_FILE.put( l_output,
                             l_delimiter || p_quotechar || l_columnvalue || p_quotechar
                           );
            END IF;

            l_delimiter := p_delimiter;
         END LOOP;

         UTL_FILE.new_line( l_output );
         l_cnt := l_cnt + 1;
      END LOOP;

      o_td.change_action( 'Close cursor and handles' );
      DBMS_SQL.close_cursor( l_thecursor );
      UTL_FILE.fclose( l_output );
      o_td.clear_app_info;
      RETURN l_cnt;
   END extract_query;

   -- uses EXTRACT_QUERY to extract the contents of an object to a file
   -- the object can be a view or a table
   FUNCTION extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   IS
      l_cnt           NUMBER           := 0;
      l_head_sql      VARCHAR( 1000 );
      l_extract_sql   VARCHAR2( 1000 );
      o_td            tdtype           := tdtype( p_module => 'extract_object' );
   BEGIN
      -- check that the source object exists and is something we can select from
      td_sql.check_object( p_owner            => p_owner,
                           p_object           => p_object,
                           p_object_type      => 'table$|view'
                         );
      l_head_sql :=
            'select regexp_replace(stragg(column_name),'','','''
         || p_delimiter
         || ''') from '
         || '(select '''
         || p_quotechar
         || '''||column_name||'''
         || p_quotechar
         || ''' as column_name'
         || ' from all_tab_cols '
         || 'where table_name='''
         || UPPER( p_object )
         || ''' and owner='''
         || UPPER( p_owner )
         || ''' order by column_id)';
      l_extract_sql := 'select * from ' || p_owner || '.' || p_object;
      td_inst.log_msg( 'Headers query: ' || l_head_sql, 3 );
      td_inst.log_msg( 'Extract query: ' || l_extract_sql, 3 );

      IF NOT td_inst.is_debugmode
      THEN
         IF td_ext.is_true( p_headers )
         THEN
            o_td.change_action( 'Extract headers to file' );
            l_cnt :=
               extract_query( p_query          => l_head_sql,
                              p_dirname        => p_dirname,
                              p_filename       => p_filename,
                              p_delimiter      => p_delimiter,
                              p_quotechar      => NULL,
                              p_append         => p_append
                            );
         END IF;

         o_td.change_action( 'Extract data to file' );
         l_cnt :=
              l_cnt
            + extract_query( p_query          => l_extract_sql,
                             p_dirname        => p_dirname,
                             p_filename       => p_filename,
                             p_delimiter      => p_delimiter,
                             p_quotechar      => p_quotechar,
                             p_append         => p_append
                           );
      END IF;

      o_td.clear_app_info;
      RETURN l_cnt;
   END extract_object;
   
   -- find records in p_source_table that match the values of the partitioned column in p_table
   -- This procedure uses an undocumented database function called tbl$or$idx$part$num.
   -- There are two "magic" numbers that are required to make it work correctly.
   -- The defaults will quite often work.
   -- The simpliest way to find which magic numbers make this function work is to
   -- do a partition exchange on the target table and trace that statement.
   PROCEDURE populate_partname(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 0,
      p_p_num           NUMBER DEFAULT 65535
   )
   AS
      l_dsql            LONG;
      l_num_msg		VARCHAR2(100) := 'Number of records inserted into TD_DDL_GTT table';
      -- to catch empty cursors
      l_source_column   all_part_key_columns.column_name%TYPE;
      l_results         NUMBER;
      o_td              tdtype                    := tdtype( p_module      => 'populate_partname' );
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_high_value      all_tab_partitions.high_value%TYPE;
   BEGIN
      td_sql.check_table( p_owner            => p_owner,
                          p_table            => p_table,
                          p_partname         => p_partname,
                          p_partitioned      => 'yes'
                        );

      IF p_partname IS NOT NULL
      THEN
         SELECT partition_position, high_value
           INTO l_part_position, l_high_value
           FROM all_tab_partitions
          WHERE table_owner = UPPER( p_owner )
            AND table_name = UPPER( p_table )
            AND partition_name = UPPER( p_partname );

         INSERT INTO partname
                ( module, action, table_owner, table_name, 
		  partition_name, partition_position
                     )
		VALUES ( td_inst.module, td_inst.action, UPPER( p_owner ), 
			 UPPER( p_table ), UPPER( p_partname ), l_part_position
                     );

         td_inst.log_cnt_msg( SQL%ROWCOUNT,
                              l_num_msg,
                              4
                            );
      ELSE
         IF p_source_column IS NULL
         THEN
            SELECT column_name
              INTO l_source_column
              FROM all_part_key_columns
             WHERE NAME = UPPER( p_table ) AND owner = UPPER( p_owner );
         ELSE
            l_source_column := p_source_column;
         END IF;
	 
         o_td.change_action( 'insert into td_ddl_gtt' );
         l_results :=
            td_sql.exec_sql
               ( p_sql      =>    'insert into td_ddl_gtt (table_owner, table_name, partition_name, partition_position) '
                               || ' SELECT table_owner, table_name, partition_name, partition_position'
                               || '  FROM all_tab_partitions'
                               || ' WHERE table_owner = '''
                               || UPPER( p_owner )
                               || ''' AND table_name = '''
                               || UPPER( p_table )
                               || ''' AND partition_position IN '
                               || ' (SELECT DISTINCT tbl$or$idx$part$num("'
                               || UPPER( p_owner )
                               || '"."'
                               || UPPER( p_table )
                               || '", 0, '
                               || p_d_num
                               || ', '
                               || p_p_num
                               || ', "'
                               || UPPER( l_source_column )
                               || '")	 FROM '
                               || UPPER( p_source_owner )
                               || '.'
                               || UPPER( p_source_object )
                               || ') '
                               || 'ORDER By partition_position'
               );
         td_inst.log_cnt_msg( l_results,
                              l_num_msg,
                              4
                            );
      END IF;

      o_td.clear_app_info;
   END populate_partname;
   
   -- function takes a text string and a delimiter and parses the string
   -- should only be used as a pipelined table function
   FUNCTION split
      (
	p_text      VARCHAR2,
	p_delimiter VARCHAR2 := ','
      ) 
      RETURN t_split PIPELINED
   IS
      l_idx    pls_integer;
      l_list   VARCHAR2(32767) := p_text;
      l_value  VARCHAR2(32767);
   BEGIN
      LOOP
	 l_idx := instr(l_list,p_delimiter);
	 IF l_idx > 0 THEN
            pipe ROW(substr(l_list,1,l_idx-1));
            l_list := substr(l_list,l_idx+length(p_delimiter));

	 ELSE
            pipe ROW(l_list);
            EXIT;
	 END IF;
      END LOOP;
      RETURN;
   END split;
   
END td_core;
/