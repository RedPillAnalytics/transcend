DROP TABLE registration_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE registration_conf
       ( registration  	     VARCHAR2(10) NOT NULL,
	 module 	     VARCHAR2(48),
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE registration_conf ADD (
  CONSTRAINT registration_conf_pk
 PRIMARY KEY
 (module)
    USING INDEX)
/

ALTER TABLE registration_conf ADD CONSTRAINT registration_conf_ck1 CHECK (module=lower(module));
ALTER TABLE registration_conf ADD CONSTRAINT registration_conf_ck2 CHECK (registration=lower(registration));
