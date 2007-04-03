DROP TABLE tdinc.notify_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.notify_conf_seq
/

CREATE TABLE tdinc.notify_conf
       ( notify_id	NUMBER NOT NULL,
	 notify_method 	VARCHAR2(20),
	 action 	VARCHAR2(32),
	 module 	VARCHAR2(48),
	 module_id 	NUMBER,
	 message   	VARCHAR2(2000),
	 subject   	VARCHAR2(100),
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
/

COMMENT ON TABLE tdinc.notify_conf IS 'table to hold generic notification information regardless of the notification method';

COMMENT ON COLUMN tdinc.notify_conf.message IS 'The default message body text. This is text that shows up in the email, but the module that sends the notification might add additional information to it';
COMMENT ON COLUMN tdinc.notify_conf.subject IS 'The default subject text. This is text that shows up in the email, but the module that sends the notification might add additional information to it';
COMMENT ON COLUMN tdinc.notify_conf.notify_method IS 'the type of notification; currently, only "email" is supported. This column also points to the supporting conf table.';

ALTER TABLE tdinc.notify_conf ADD (
  CONSTRAINT notify_conf_pk
 PRIMARY KEY
 (notify_id)
    USING INDEX)
/

CREATE SEQUENCE tdinc.notify_conf_seq
/