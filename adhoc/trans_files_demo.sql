
-- create test directories
CREATE OR replace directory extdata AS '/transcend/ext';
GRANT READ,WRITE ON directory extdata TO tdrep;

CREATE OR replace directory archdata AS '/transcend/arch';
GRANT READ,WRITE ON directory archdata TO tdrep;

CREATE OR REPLACE directory extractdata AS '/transcend/extract';
GRANT READ,WRITE ON directory extractdata TO tdrep;

CREATE OR REPLACE directory sourcedata AS '/transcend/source';
GRANT READ,WRITE ON directory extractdata TO tdrep;


-- create test external table
DROP TABLE test_feed
/
CREATE TABLE test_feed
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

-- CREATE a test feed
EXEC trans_adm.configure_feed( 'test group','test feed',p_filename=>'TEST_FEED.dat',p_owner=>'stewart',p_table=>'test_feed',p_arch_directory=>'archdata', p_file_datestamp=>'yyyymmddhhmiss',p_baseurl=>'www.transcendentdata.com/files',p_passphrase=>'passw0rd',p_source_directory=>'sourcedata',p_source_regexp=>'txt$',p_source_policy=>'newest',p_delete_source=>'no');

-- CREATE a test extract
EXEC trans_adm.configure_extract( 'test group','test extract',p_filename=>'TEST_EXTRACT.dat',p_object_owner=>'stewart',p_object_name=>'test_extract',p_arch_directory=>'archdata',p_baseurl=>'www.transcendentdata.com/files',p_passphrase=>'passw0rd',p_directory=>'extractdata');