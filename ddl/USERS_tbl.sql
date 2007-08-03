DROP TABLE users CASCADE CONSTRAINTS purge
/

CREATE TABLE users
       ( user_name           VARCHAR2(30) NOT NULL,
	 application_name    VARCHAR2(30) NOT NULL,
	 repository_name     VARCHAR2(30) NOT NULL,
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       )
/

ALTER TABLE users ADD (
  CONSTRAINT users_pk
 PRIMARY KEY
 (user_name)
    USING INDEX
)
/

ALTER TABLE users ADD (
      CONSTRAINT users_fk1
      FOREIGN KEY (repository_name)
      REFERENCES repositories  
      ( repository_name )
)
/

ALTER TABLE users ADD (
      CONSTRAINT users_fk2
      FOREIGN KEY (application_name)
      REFERENCES applications 
      ( application_name )
)
/
