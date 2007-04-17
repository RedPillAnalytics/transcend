DROP TABLE tdinc.registration_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.registration_conf_seq
/

CREATE TABLE tdinc.registration_conf
       ( registration_id     NUMBER NOT NULL,
	 registration  	     VARCHAR2(10) not NULL,
	 action 	     VARCHAR2(32),
	 module 	     VARCHAR2(48),
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE tdinc.registration_conf ADD (
  CONSTRAINT registration_conf_pk
 PRIMARY KEY
 (registration_id)
    USING INDEX)
/

CREATE SEQUENCE tdinc.registration_conf_seq
/

INSERT INTO tdinc.registration_conf
       ( registration_id, 
	 registration, 
	 action, 
	 module, 
	 created_user, 
	 created_dt, 
	 modified_user, 
	 modified_dt
       )
       VALUES ( tdinc.registration_conf_seq.nextval,
		'register',
		'default',
		'default',
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);
