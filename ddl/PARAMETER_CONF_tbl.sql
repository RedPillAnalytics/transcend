DROP TABLE tdinc.parameter_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.parameter_conf_seq
/

CREATE TABLE tdinc.parameter_conf
       ( parameter_id	NUMBER NOT NULL,
	 name VARCHAR2(40),
	 value VARCHAR2(40),
	 module 	VARCHAR2(48),
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
)
/

COMMENT ON TABLE tdinc.parameter_conf IS 'table to hold parameters for the entire framework';

ALTER TABLE tdinc.parameter_conf ADD (
  CONSTRAINT parameter_conf_pk
 PRIMARY KEY
 (parameter_id)
    USING INDEX)
/

CREATE SEQUENCE tdinc.parameter_conf_seq
/