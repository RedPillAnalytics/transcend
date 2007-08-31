DROP TABLE column_type_list CASCADE CONSTRAINTS purge
/

CREATE TABLE column_type_list
       ( column_type		VARCHAR2(30) NOT NULL,
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE column_type_list IS 'constraining table for the COLUMN_TYPE column in COLUMN_CONF';

COMMENT ON COLUMN column_type_list.column_type IS 'column to constrain';
COMMENT ON COLUMN column_type_list.created_user IS 'for auditing';
COMMENT ON COLUMN column_type_list.created_dt IS 'for auditing';
COMMENT ON COLUMN column_type_list.modified_user IS 'for auditing';
COMMENT ON COLUMN column_type_list.modified_dt IS 'for auditing';

ALTER TABLE column_type_list 
      ADD (
	    CONSTRAINT column_type_list_pk
	    PRIMARY KEY
	    ( column_type_list )
	    USING INDEX
	  )
/

INSERT INTO column_type_list (column_type) VALUES ('surrogate key');
INSERT INTO column_type_list (column_type) VALUES ('natural key');
INSERT INTO column_type_list (column_type) VALUES ('scd type 1');
INSERT INTO column_type_list (column_type) VALUES ('scd type 2');
INSERT INTO column_type_list (column_type) VALUES ('scd type 3');
INSERT INTO column_type_list (column_type) VALUES ('effective start date');
INSERT INTO column_type_list (column_type) VALUES ('effective end date');
INSERT INTO column_type_list (column_type) VALUES ('current indicator');
