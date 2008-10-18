
-- create test directories
CREATE OR replace directory extdata AS '/transcend/ext';
GRANT READ,WRITE ON directory extdata TO stewart;

CREATE OR replace directory workdata AS '/transcend/work';
GRANT READ,WRITE ON directory workdata TO stewart;

CREATE OR REPLACE directory extractdata AS '/transcend/extract';
GRANT READ,WRITE ON directory extractdata TO stewart;

CREATE OR REPLACE directory sourcedata AS '/transcend/source';
GRANT READ,WRITE ON directory sourcedata TO stewart;

CREATE VIEW stewart.test_extract AS SELECT * FROM user_tables;


-- create test external table
DROP TABLE stewart.test_feed
/
CREATE TABLE stewart.test_feed
       ( feed_id        NUMBER,
	 feed_desc     VARCHAR2(100))
       organization external
       ( TYPE oracle_loader
	 DEFAULT directory extdata
	 ACCESS parameters
	 ( fields terminated BY '|'
	   ( feed_ID,
	     feed_desc)
	 )
	 location ('TEST_FEED.dat'))
       reject limit UNLIMITED
       PARALLEL
/

exec dbms_java.grant_permission( 'TDREP', 'SYS:java.io.FilePermission', '/transcend/source', 'read' );

EXEC dbms_java.grant_permission( 'TDREP', 'SYS:java.io.FilePermission', '/transcend/source/*', 'read' );

-- CREATE a test feed
EXEC trans_adm.create_feed( 'test feed','test group',p_directory=>'extdata',p_filename=>'TEST_FEED.dat',p_owner=>'stewart',p_table=>'test_feed',p_work_directory=>'workdata', p_baseurl=>'www.transcendentdata.com/files', p_passphrase=>'passw0rd',p_source_directory=>'sourcedata',p_source_regexp=>'\.txt',p_source_policy=>'newest',p_delete_source=>'no', p_compress_method=> trans_adm.gzip_method, p_required=>'no');

-- CREATE a test extract
EXEC trans_adm.create_extract( 'test extract','test group',p_filename=>'TEST_EXTRACT.dat',p_object_owner=>'stewart',p_object_name=>'test_extract',p_work_directory=>'workdata',p_baseurl=>'www.transcendentdata.com/files',p_directory=>'extractdata');

exec evolve_adm.set_command_conf(p_name=>'gunzip',p_path=>'/usr/bin');

COMMIT;

--EXEC trans_files.process_group( 'test group' );