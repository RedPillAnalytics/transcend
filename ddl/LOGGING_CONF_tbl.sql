DROP TABLE tdinc.logging_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.logging_conf_seq
/

CREATE TABLE tdinc.logging_conf
       ( logging_id	  NUMBER NOT NULL,
	 logging_level    NUMBER not NULL,
	 debug_level 	  NUMBER NOT NULL,
	 action 	  VARCHAR2(32),
	 module 	  VARCHAR2(48),
	 created_user     VARCHAR2(30) NOT NULL,
	 created_dt       DATE NOT NULL,
	 modified_user    VARCHAR2(30),
	 modified_dt	  DATE
       )
/

COMMENT ON TABLE tdinc.logging_conf IS 'table to hold generic notification information regardless of the notification method';

COMMENT ON COLUMN tdinc.logging_conf.message IS 'The default message body text. This is text that shows up in the email, but the module that sends the notification might add additional information to it';
COMMENT ON COLUMN tdinc.logging_conf.subject IS 'The default subject text. This is text that shows up in the email, but the module that sends the notification might add additional information to it';
COMMENT ON COLUMN tdinc.logging_conf.notify_method IS 'the type of notification; currently, only "email" is supported. This column also points to the supporting conf table.';

ALTER TABLE tdinc.logging_conf ADD (
  CONSTRAINT logging_conf_pk
 PRIMARY KEY
 (logging_id)
    USING INDEX)
/

CREATE SEQUENCE tdinc.logging_conf_seq
/

INSERT INTO tdinc.logging_conf
       ( logging_id, 
	 logging_level, 
	 debug_level, 
	 action, 
	 module, 
	 created_user, 
	 created_dt, 
	 modified_user, 
	 modified_dt
       )
       VALUES ( tdinc.logging_conf_seq.nextval,
		2,
		4,
		null,
		null,
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);
