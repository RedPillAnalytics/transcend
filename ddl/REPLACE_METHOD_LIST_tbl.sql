DROP TABLE replace_method_list CASCADE CONSTRAINTS purge
/

CREATE TABLE replace_method_list
       ( 
	 replace_method		VARCHAR2(10) NOT NULL,
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE replace_method_list IS 'used for foreign keys to constrain the list of applicable values for REPLACE_METHOD';

COMMENT ON COLUMN replace_method_list.replace_method IS 'list of applicable values for REPLACE_METHOD';
COMMENT ON COLUMN replace_method_list.created_user IS 'for auditing';
COMMENT ON COLUMN replace_method_list.created_dt IS 'for auditing';
COMMENT ON COLUMN replace_method_list.modified_user IS 'for auditing';
COMMENT ON COLUMN replace_method_list.modified_dt IS 'for auditing';

ALTER TABLE replace_method_list 
      ADD (
	    CONSTRAINT replace_method_list_pk
	    PRIMARY KEY
	    ( replace_method )
	    USING INDEX
	  )
/

INSERT INTO replace_method_list (replace_method) VALUES ('exchange');
INSERT INTO replace_method_list (replace_method) VALUES ('insert');
INSERT INTO replace_method_list (replace_method) VALUES ('merge');
INSERT INTO replace_method_list (replace_method) VALUES ('rename');
