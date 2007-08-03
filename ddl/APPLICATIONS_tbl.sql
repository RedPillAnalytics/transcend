DROP TABLE applications CASCADE CONSTRAINTS purge
/

DROP SEQUENCE applications_seq
/

CREATE TABLE applications
       ( application_id      NUMBER NOT NULL,
	 application_schema  VARCHAR2(30) NOT NULL,
	 repository_schema   VARCHAR2(30) NOT NULL,
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE applications ADD (
  CONSTRAINT applications_pk
 PRIMARY KEY
 (application_id)
    USING INDEX)
/

CREATE SEQUENCE applications_seq
/