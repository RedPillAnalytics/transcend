DROP TABLE parameter_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE parameter_conf_seq
/

CREATE TABLE parameter_conf
       ( parameter_id	NUMBER NOT NULL,
	 name		VARCHAR2(40) NOT NULL,
	 value 		VARCHAR2(40),
	 module 	VARCHAR2(48) NOT NULL,
	 created_user   VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
/

COMMENT ON TABLE parameter_conf IS 'table to hold session level parameters';

ALTER TABLE parameter_conf ADD (
  CONSTRAINT parameter_conf_pk
 PRIMARY KEY
 (parameter_id)
    USING INDEX)
/

-- make sure that 'default' is not added here
ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck1 CHECK (lower(value) <> 'default');

CREATE SEQUENCE parameter_conf_seq
/
