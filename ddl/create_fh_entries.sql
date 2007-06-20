CREATE OR replace directory extdata AS '/transcend/ext';
GRANT READ,WRITE ON directory extdata TO tdinc;

CREATE OR replace directory archdata AS '/transcend/arch';
GRANT READ,WRITE ON directory archdata TO tdinc;

CREATE OR REPLACE directory extractdata AS '/transcend/extract';
GRANT READ,WRITE ON directory extractdata TO tdinc;

CREATE OR REPLACE directory sourcedata AS '/transcend/source';
GRANT READ,WRITE ON directory extractdata TO tdinc;

TRUNCATE TABLE tdinc.filehub_conf;

DROP TABLE tdinc.test_feed
/
CREATE TABLE tdinc.test_feed
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

INSERT INTO tdinc.filehub_conf
       ( filehub_id, 
	 filehub_name, 
	 filehub_group, 
	 filehub_type, 
	 object_owner, 
	 object_name, 
	 directory, 
	 filename, 
	 arch_directory, 
	 min_bytes, 
	 max_bytes, 
	 file_datestamp, 
	 baseurl, 
	 source_directory, 
	 source_regexp, 
	 regexp_options, 
	 source_policy, 
	 required, 
	 dateformat, 
	 timestampformat, 
	 delimiter, 
	 quotechar, 
	 headers)
       VALUES
       ( tdinc.filehub_conf_seq.nextval,
	 'Test Feed',
	 'Test Group',
	 'feed',
	 'tdinc',
	 'test_feed',
	 'EXTDATA',
	 'TEST_FEED.dat',
	 'ARCHDATA',
	 1,
	 10000000,
	 'mmddyyyyhhmiss',
	 'www.transcendentdata.com/feeds',
	 'SOURCEDATA',
	 '^test.+\.txt$',
	 'i',
	 'all',
	 'Y',
	 null,
	 null,
	 null,
	 null,
	 null);


-- INSERT INTO tdinc.notify_conf
--        VALUES ( tdinc.notify_conf_seq.nextval,
-- 		'email',
-- 		'yes',
-- 		'Notify success',
-- 		'feed.process',
-- 		tdinc.filehub_conf_seq.currval,
-- 		'test feed file received',
-- 		'test feed file received',
-- 		'stewart.bryson@transcendentdat.com',
-- 		'stewartbryson@gmail.com,stewart.bryson@transcendentdata.com',
-- 		sys_context('USERENV','SESSION_USER'),
-- 		SYSDATE,
-- 		NULL,
-- 		NULL);

-- INSERT INTO tdinc.notify_conf
--        VALUES ( tdinc.notify_conf_seq.nextval,
-- 		'email',
-- 		'yes',
-- 		'reject limit exceeded',
-- 		'feed.process',
-- 		tdinc.filehub_conf_seq.currval,
-- 		'Source file reject limit exceeded',
-- 		'Source file reject limit exceeded',
-- 		'stewart.bryson@transcendentdat.com',
-- 		'stewartbryson@gmail.com,stewart.bryson@transcendentdata.com',
-- 		sys_context('USERENV','SESSION_USER'),
-- 		SYSDATE,
-- 		NULL,
-- 		NULL);


INSERT INTO tdinc.filehub_conf
       ( filehub_id, 
	 filehub_name, 
	 filehub_group, 
	 filehub_type, 
	 object_owner, 
	 object_name, 
	 directory, 
	 filename, 
	 arch_directory, 
	 min_bytes, 
	 max_bytes, 
	 file_datestamp, 
	 baseurl, 
	 source_directory, 
	 source_regexp, 
	 regexp_options, 
	 source_policy, 
	 required, 
	 dateformat, 
	 timestampformat, 
	 delimiter, 
	 quotechar, 
	 headers)
       VALUES
       ( tdinc.filehub_conf_seq.nextval,
	 'Test Extract',
	 'Test Group',
	 'extract',
	 'tdinc',
	 'filehub_conf',
	 'EXTRACTDATA',
	 'filehub_conf.csv',
	 'ARCHDATA',
	 1,
	 10000000,
	 'mmddyyyyhhmiss',
	 'www.transcendentdata.com/extracts',
	 NULL,
	 NULL,
	 NULL,
	 NULL,
	 NULL,
	 'mmddyyyyhhmiss',
	 'mmddyyyyhhmiss',
	 ',',
	 '"',
	 'Y');
