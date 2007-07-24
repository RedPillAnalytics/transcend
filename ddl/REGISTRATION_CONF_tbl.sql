DROP TABLE registration_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE registration_conf_seq
/

CREATE TABLE registration_conf
       ( registration_id     NUMBER NOT NULL,
	 registration  	     VARCHAR2(10) NOT NULL,
	 module 	     VARCHAR2(48),
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE registration_conf ADD (
  CONSTRAINT registration_conf_pk
 PRIMARY KEY
 (registration_id)
    USING INDEX)
/

CREATE SEQUENCE registration_conf_seq
/
