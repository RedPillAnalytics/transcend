
-- reset
exec trans_adm.delete_feed( p_file_label => 'td_file');
exec trans_adm.delete_extract( p_file_label => 'td_extract');

-- create external table
CREATE TABLE td_demo.td_file 
       (
         lineid         NUMBER(3),
         linetext       VARCHAR2(30)
       )
       organization external 
       (
         TYPE oracle_loader
         DEFAULT directory td_files
         ACCESS parameters 
         (
           records delimited BY newline
           badfile 'bad_%a_%p.bad'
           LOGFILE 'log_%a_%p.log'
           fields terminated BY '|'
           missing field VALUES are NULL
           reject ROWS WITH ALL NULL fields
           ( lineid    INTEGER external(3),
             linetext  CHAR(30)
           )
         )
         location ('test.txt')
       )
       PARALLEL
       reject limit UNLIMITED
/

-- create transcend feed

BEGIN
   trans_adm.create_feed
   ( -- file label and group
     p_file_label       => 'td_file',
     p_file_group       => 'td_demo_files',
     -- oracle directories
     p_directory        => 'td_files',
     p_source_directory => 'td_source',
     p_source_regexp    => 'test\d\.txt',
     -- external table
     p_owner            => 'td_demo',
     p_table            => 'td_file',
     -- file parameters
     p_filename         => 'file.txt',
     p_source_policy    => 'newest',
     p_delete_source    => 'no',
     p_description      => 'test feed for demo'
   );
END;
/

COMMIT;


-- process the file

BEGIN
   trans_files.process_file
   ( 
     p_file_label       => 'td_file'
   );
END;
/


select * 
  from td_demo.td_file;


-- see the file detail table
SELECT *
  FROM file_detail_v;


-- change source policy for oldest

BEGIN
   trans_adm.modify_feed
   ( 
     p_file_label       => 'td_file',
     p_source_policy    => 'oldest'
   );
END;
/

COMMIT;


-- process the file

BEGIN
   trans_files.process_file
   ( 
     p_file_label       => 'td_file'
   );
END;
/


select * 
  from td_demo.td_file;


-- change source policy for oldest

BEGIN
   trans_adm.modify_feed
   ( 
     p_file_label       => 'td_file',
     p_source_policy    => 'all'
   );
END;
/

COMMIT;


-- process the file

BEGIN
   trans_files.process_file
   ( 
     p_file_label       => 'td_file'
   );
END;
/


select * 
  from td_demo.td_file;


-- take a look at the external table
SELECT dbms_metadata.get_ddl('TABLE', 'TD_FILE', 'TD_DEMO') AS ddl
  FROM dual;


-- create an extract

BEGIN
   trans_adm.create_extract
   ( -- file label and group
     p_file_label       => 'td_extract',
     p_file_group       => 'td_demo_files',
     -- oracle directories
     p_directory        => 'td_files',
     -- external table
     p_object_owner     => 'tdrep',
     p_object_name      => 'log',
     -- file parameters
     p_filename         => 'log_dump.txt',
     p_description      => 'test feed for demo'
   );
END;
/

COMMIT;


SELECT *
  FROM file_conf;


-- process the file group

BEGIN
   trans_files.process_group
   ( 
     p_file_group       => 'td_demo_files'
   );
END;
/

-- end demo