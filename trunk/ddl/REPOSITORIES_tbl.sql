CREATE TABLE repositories
       ( repository_name     VARCHAR2(30) NOT NULL,
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE repositories ADD (
  CONSTRAINT repositories_pk
 PRIMARY KEY
 (repository_name)
    USING INDEX)
/
