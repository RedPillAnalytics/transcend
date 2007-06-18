DROP TABLE parameter_conf CASCADE CONSTRAINTS purge
/

DROP SEQUENCE parameter_conf_seq
/

CREATE TABLE parameter_conf
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

COMMENT ON TABLE parameter_conf IS 'table to hold parameters for the entire framework';

ALTER TABLE parameter_conf ADD (
  CONSTRAINT parameter_conf_pk
 PRIMARY KEY
 (parameter_id)
    USING INDEX)
/

CREATE SEQUENCE parameter_conf_seq
/

ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_uk1 UNIQUE (name) USING INDEX;
ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck1 CHECK (REGEXP_LIKE(name,'[[:lower:]]+'));
ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck2 CHECK (REGEXP_LIKE(value,'([[:lower:]]|[[:digit:]])+'));

INSERT INTO parameter_conf (parameter_id,module,name,value,created_user,created_dt) VALUES (parameter_conf_seq.nextval,'system','runmode','runtime',sys_context('USERENV','SESSION_USER'),SYSDATE);
INSERT INTO parameter_conf (parameter_id,module,name,value,created_user,created_dt) VALUES (parameter_conf_seq.nextval,'system','registration','register',sys_context('USERENV','SESSION_USER'),SYSDATE);
INSERT INTO parameter_conf (parameter_id,module,name,value,created_user,created_dt) VALUES (parameter_conf_seq.nextval,'system','logging_level','2',sys_context('USERENV','SESSION_USER'),SYSDATE);
