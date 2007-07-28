DROP TABLE tdinc.email_notify_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.email_notify_conf_seq
/

CREATE TABLE tdinc.email_notify_conf
       ( email_notify_id NUMBER NOT NULL,
	 notify_id	 NUMBER,
	 sender         VARCHAR2(1024) NOT NULL,
	 recipients     VARCHAR2(2000) NOT NULL,
	 created_user   VARCHAR2(30),
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
/

ALTER TABLE tdinc.email_notify_conf ADD (
  CONSTRAINT email_notify_conf_pk
 PRIMARY KEY
 (email_notify_id)
    USING INDEX)
/

CREATE SEQUENCE tdinc.email_notify_conf_seq
/