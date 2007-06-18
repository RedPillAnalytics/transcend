DROP TABLE notify_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE notify_conf_seq
/

CREATE TABLE notify_conf
       ( notify_id	  NUMBER NOT NULL,
	 notify_method 	  VARCHAR2(20) NOT NULL,
	 notify_enabled	  VARCHAR2(3) NOT NULL,
	 message          VARCHAR2(2000),
	 subject   	  VARCHAR2(100),
	 sender           VARCHAR2(1024) NOT NULL,
	 recipients       VARCHAR2(2000) NOT NULL,
	 action 	  VARCHAR2(32),
	 module 	  VARCHAR2(48),
	 module_id 	  NUMBER,
	 created_user     VARCHAR2(30) NOT NULL,
	 created_dt       DATE NOT NULL,
	 modified_user    VARCHAR2(30),
	 modified_dt	  DATE
       )
/

COMMENT ON TABLE notify_conf IS 'table to hold generic notification information regardless of the notification method';

COMMENT ON COLUMN notify_conf.message IS 'The default message body text. This is text that shows up in the email, but the module that sends the notification might add additional information to it';
COMMENT ON COLUMN notify_conf.subject IS 'The default subject text. This is text that shows up in the email, but the module that sends the notification might add additional information to it';
COMMENT ON COLUMN notify_conf.notify_method IS 'the type of notification; currently, only "email" is supported. This column also points to the supporting conf table.';

ALTER TABLE notify_conf ADD (
  CONSTRAINT notify_conf_pk
 PRIMARY KEY
 (notify_id)
    USING INDEX)
/

CREATE SEQUENCE notify_conf_seq
/

INSERT INTO notify_conf
       ( notify_id, 
	 notify_method, 
	 notify_enabled, 
	 message, 
	 subject, 
	 sender, 
	 recipients, 
	 action, 
	 module, 
	 module_id, 
	 created_user, 
	 created_dt, 
	 modified_user, 
	 modified_dt
       ) 
       VALUES ( notify_conf_seq.nextval,
		'email',
		'yes',
		'test feed file received',
		'test feed file received',
		'stewart.bryson@transcendentdat.com',
		'stewartbryson@gmail.com,stewart.bryson@transcendentdata.com',
		'notify success',
		'feed.process',
		null,
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);

INSERT INTO notify_conf
       ( notify_id, 
	 notify_method, 
	 notify_enabled, 
	 message, 
	 subject, 
	 sender, 
	 recipients, 
	 action, 
	 module, 
	 module_id, 
	 created_user, 
	 created_dt, 
	 modified_user, 
	 modified_dt
       ) 
       VALUES ( notify_conf_seq.nextval,
		'email',
		'yes',
		'Source file reject limit exceeded',
		'Source file reject limit exceeded',
		'stewart.bryson@transcendentdat.com',
		'stewartbryson@gmail.com,stewart.bryson@transcendentdata.com',
		'reject limit exceeded',
		'feed.process',
		null,
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);

INSERT INTO notify_conf
       ( notify_id, 
	 notify_method, 
	 notify_enabled, 
	 message, 
	 subject, 
	 sender, 
	 recipients, 
	 action, 
	 module, 
	 module_id, 
	 created_user, 
	 created_dt, 
	 modified_user, 
	 modified_dt
       ) 
       VALUES ( notify_conf_seq.nextval,
		'email',
		'yes',
		'test extract ready for download',
		'test extract ready for download',
		'stewart.bryson@transcendentdat.com',
		'stewartbryson@gmail.com,stewart.bryson@transcendentdata.com',
		'notify success',
		'extract.process',
		null,
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);