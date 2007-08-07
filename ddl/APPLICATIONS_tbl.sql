CREATE TABLE applications
       ( application_name    VARCHAR2(30) NOT NULL,
	 repository_name     VARCHAR2(30) NOT NULL,
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE applications ADD (
  CONSTRAINT applications_pk
 PRIMARY KEY
 (application_name)
    USING INDEX
)
/

ALTER TABLE applications ADD (
      CONSTRAINT applications_fk1
      FOREIGN KEY (repository_name)
      REFERENCES repositories  
      ( repository_name )
)
/
