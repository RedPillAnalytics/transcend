DROP TABLE logging_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE logging_conf_seq
/

CREATE TABLE logging_conf
       ( logging_id	  NUMBER NOT NULL,
	 logging_level    NUMBER not NULL,
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
 (logging_id)
    USING INDEX)
/

CREATE SEQUENCE logging_conf_seq
/
