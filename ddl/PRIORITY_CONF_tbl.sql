DROP TABLE priority_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE priority_conf
       ( accessor	VARCHAR2(30) NOT NULL,
	 priority 	NUMBER NOT NULL,
	 created_user   VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
/

COMMENT ON TABLE priority_conf IS 'table to hold the priorities used in setting values in the TD_INST package';

ALTER TABLE priority_conf ADD (
  CONSTRAINT priority_conf_pk
 PRIMARY KEY
 (accessor)
    USING INDEX)
/
