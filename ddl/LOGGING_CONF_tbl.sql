DROP TABLE tdinc.logging_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.logging_conf_seq
/

CREATE TABLE tdinc.logging_conf
       ( logging_id	  NUMBER NOT NULL,
	 logging_level    NUMBER not NULL,
	 debug_level 	  NUMBER NOT NULL,
	 action 	  VARCHAR2(32) NOT NULL,
	 module 	  VARCHAR2(48) NOT NULL,
	 created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt       DATE DEFAULT SYSDATE NOT NULL,
	 modified_user    VARCHAR2(30),
	 modified_dt	  DATE
       )
/

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
		'default',
		'default',
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);
