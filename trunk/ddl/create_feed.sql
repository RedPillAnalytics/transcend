CREATE or REPLACE directory extdata AS '/transcend/ext';
GRANT READ,WRITE ON directory extdata TO tdinc;

TRUNCATE TABLE tdinc.email_notify_conf;
DELETE (SELECT * FROM tdinc.filehub_conf);
DELETE (SELECT * FROM tdinc.notify_conf);

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


INSERT INTO tdinc.email_notify_conf
       VALUES ( tdinc.email_notify_conf_seq.nextval,
		'no-reply@transcendentdat.com',
		'stewartbryson@gmail.com,stewart.bryson@transcendentdata.com',
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);

INSERT INTO tdinc.notify_conf
       VALUES ( tdinc.notify_conf_seq.nextval,
		'email',
		tdinc.email_notify_conf_seq.currval,
		'test feed file received',
		'test feed file received',
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);

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
	 notify_id, 
	 notify,
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
	 tdinc.notify_conf_seq.currval,
	 'Y',
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
