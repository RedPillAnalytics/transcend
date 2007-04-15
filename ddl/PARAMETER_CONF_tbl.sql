DROP TABLE tdinc.parameter_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.parameter_conf_seq
/

CREATE TABLE tdinc.parameter_conf
       ( parameter_id	NUMBER NOT NULL,
	 module 	VARCHAR2(48) NOT NULL,
	 action		VARCHAR2(32),
	 name 		VARCHAR2(40) NOT NULL,
	 value 		VARCHAR2(40) NOT NULL,
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

ALTER TABLE tdinc.parameter_conf ADD CONSTRAINT parameter_conf_uk1 UNIQUE (name) USING INDEX;
ALTER TABLE tdinc.parameter_conf ADD CONSTRAINT parameter_conf_ck1 CHECK (REGEXP_LIKE(name,'[[:lower:]]+'));
ALTER TABLE tdinc.parameter_conf ADD CONSTRAINT parameter_conf_ck2 CHECK (REGEXP_LIKE(value,'([[:lower:]]|[[:digit:]])+'));

INSERT INTO tdinc.parameter_conf (parameter_id,module,name,value,created_user,created_dt) VALUES (tdinc.parameter_conf_seq.nextval,'system','runmode','runtime',sys_context('USERENV','SESSION_USER'),SYSDATE);
INSERT INTO tdinc.parameter_conf (parameter_id,module,name,value,created_user,created_dt) VALUES (tdinc.parameter_conf_seq.nextval,'system','registration','register',sys_context('USERENV','SESSION_USER'),SYSDATE);
INSERT INTO tdinc.parameter_conf (parameter_id,module,name,value,created_user,created_dt) VALUES (tdinc.parameter_conf_seq.nextval,'system','logging_level','2',sys_context('USERENV','SESSION_USER'),SYSDATE);
