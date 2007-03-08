DROP TABLE tdinc.notification CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.notification_seq
/

CREATE TABLE tdinc.notification
       ( notification_id NUMBER NOT NULL,
	 notification_type VARCHAR2(30) NOT NULL,
	 sender         VARCHAR2(1024) NOT NULL,
	 recipients     VARCHAR2(2000) NOT NULL,
	 subject VARCHAR2(100) NOT NULL,
	 message VARCHAR2(2000) NOT NULL,
	 baseurl        VARCHAR2(255) NOT NULL,
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
TABLESPACE tdinc
/

ALTER TABLE tdinc.notification ADD (
  CONSTRAINT notification_pk
 PRIMARY KEY
 (notification_id)
    USING INDEX
    TABLESPACE tdinc)
/

CREATE SEQUENCE tdinc.notification_seq
/