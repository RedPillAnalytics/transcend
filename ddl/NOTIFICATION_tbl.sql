DROP TABLE tdinc.email_notify CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.email_notify_seq
/

CREATE TABLE tdinc.email_notify
       ( email_notify_id NUMBER NOT NULL,
	 sender         VARCHAR2(1024) NOT NULL,
	 recipients     VARCHAR2(2000) NOT NULL,
	 subject VARCHAR2(100) NOT NULL,
	 message VARCHAR2(2000) NOT NULL,
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
TABLESPACE tdinc
/

ALTER TABLE tdinc.email_notify ADD (
  CONSTRAINT notification_pk
 PRIMARY KEY
 (email_notify_id)
    USING INDEX
    TABLESPACE tdinc)
/

CREATE SEQUENCE tdinc.email_notify_seq
/