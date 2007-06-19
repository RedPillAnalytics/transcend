DROP TABLE parameter_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE parameter_conf_seq
/

CREATE TABLE parameter_conf
       ( parameter_id	NUMBER NOT NULL,
	 name		VARCHAR2(40) NOT NULL,
	 value 		VARCHAR2(40),
	 action		VARCHAR2(32),
	 module 	VARCHAR2(48) NOT NULL,
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
/

COMMENT ON TABLE parameter_conf IS 'table to hold parameters for the entire framework';

ALTER TABLE parameter_conf ADD (
  CONSTRAINT parameter_conf_pk
 PRIMARY KEY
 (parameter_id)
    USING INDEX)
/

CREATE SEQUENCE parameter_conf_seq
/
