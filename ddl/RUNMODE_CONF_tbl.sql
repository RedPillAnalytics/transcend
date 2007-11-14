DROP TABLE runmode_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE runmode_conf
       ( default_runmode  VARCHAR2(10) not NULL,
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
 (module)
    USING INDEX)
/
