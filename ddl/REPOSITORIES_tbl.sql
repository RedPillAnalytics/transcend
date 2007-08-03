DROP TABLE repositories CASCADE CONSTRAINTS purge
/

DROP SEQUENCE repositories_seq
/

CREATE TABLE repositories
       ( repository_id       NUMBER NOT NULL,
	 repository_schema   VARCHAR2(30) NOT NULL,
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE repositories ADD (
  CONSTRAINT repositories_pk
 PRIMARY KEY
 (repository_id)
    USING INDEX)
/

CREATE SEQUENCE repositories_seq
/
