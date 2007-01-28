DROP TABLE tdinc.email_notification CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.email_notification_seq
/

CREATE TABLE tdinc.email_notification
       ( email_notification_id NUMBER NOT NULL,
	 module VARCHAR2(48) NOT NULL,
	 module_id NUMBER NOT NULL,
	 notification_type VARCHAR2(30) NOT NULL,
	 sender         VARCHAR2(1024) NOT NULL,
	 recipients     VARCHAR2(2000) NOT NULL,
	 baseurl        VARCHAR2(255) NOT NULL,
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
TABLESPACE tdinc
/

ALTER TABLE tdinc.email_notification ADD (
  CONSTRAINT email_notification_pk
 PRIMARY KEY
 (email_notification_id)
    USING INDEX
    TABLESPACE tdinc)
/

CREATE SEQUENCE tdinc.email_notification_seq
/