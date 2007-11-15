DROP TABLE logging_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE logging_conf
       ( logging_level    NUMBER not NULL,
	 debug_level 	  NUMBER NOT NULL,
	 module 	  VARCHAR2(48) NOT NULL,
	 created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt       DATE DEFAULT SYSDATE NOT NULL,
	 modified_user    VARCHAR2(30),
	 modified_dt	  DATE
       )
/

ALTER TABLE logging_conf ADD (
  CONSTRAINT logging_conf_pk
 PRIMARY KEY
 (module)
    USING INDEX)
/

ALTER TABLE runmode_conf ADD CONSTRAINT runmode_conf_ck1 CHECK (module=lower(module));
