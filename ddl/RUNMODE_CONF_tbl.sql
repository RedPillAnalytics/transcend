DROP TABLE runmode_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE runmode_conf_seq
/

CREATE TABLE runmode_conf
       ( runmode_id	  NUMBER NOT NULL,
	 default_runmode  VARCHAR2(10) not NULL,
	 module 	  VARCHAR2(48),
	 created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt       DATE DEFAULT SYSDATE NOT NULL,
	 modified_user    VARCHAR2(30),
	 modified_dt	  DATE
       )
/

ALTER TABLE runmode_conf ADD (
  CONSTRAINT runmode_conf_pk
 PRIMARY KEY
 (runmode_id)
    USING INDEX)
/

CREATE SEQUENCE runmode_conf_seq
/

INSERT INTO runmode_conf
       ( runmode_id, 
	 default_runmode, 
	 module, 
	 created_user, 
	 created_dt, 
	 modified_user, 
	 modified_dt
       )
       VALUES ( runmode_conf_seq.nextval,
		'runtime',
		'default',
		sys_context('USERENV','SESSION_USER'),
		SYSDATE,
		NULL,
		NULL);
