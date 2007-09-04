DROP TABLE load_method_list CASCADE CONSTRAINTS purge
/

CREATE TABLE load_method_list
       ( 
	 load_method		VARCHAR2(10) NOT NULL,
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE load_method_list IS 'used for foreign keys to constrain the list of applicable values for LOAD_METHOD';

COMMENT ON COLUMN load_method_list.load_method IS 'list of applicable values for LOAD_METHOD';
COMMENT ON COLUMN load_method_list.created_user IS 'for auditing';
COMMENT ON COLUMN load_method_list.created_dt IS 'for auditing';
COMMENT ON COLUMN load_method_list.modified_user IS 'for auditing';
COMMENT ON COLUMN load_method_list.modified_dt IS 'for auditing';

ALTER TABLE load_method_list 
      ADD (
	    CONSTRAINT load_method_list_pk
	    PRIMARY KEY
	    ( load_method )
	    USING INDEX
	  )
/