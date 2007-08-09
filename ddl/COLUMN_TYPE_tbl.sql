DROP TABLE column_type CASCADE CONSTRAINTS purge
/

CREATE TABLE column_type
       ( column_type		VARCHAR2(30) NOT NULL,
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE column_type IS 'constraining table for the COLUMN_TYPE column in COLUMN_CONF';

COMMENT ON COLUMN column_type.column_type IS 'column to constrain';
COMMENT ON COLUMN column_type.created_user IS 'for auditing';
COMMENT ON COLUMN column_type.created_dt IS 'for auditing';
COMMENT ON COLUMN column_type.modified_user IS 'for auditing';
COMMENT ON COLUMN column_type.modified_dt IS 'for auditing';

ALTER TABLE column_type 
      ADD (
	    CONSTRAINT column_type_pk
	    PRIMARY KEY
	    ( column_type )
	    USING INDEX
	  )
/

INSERT INTO column_type (column_type) VALUES ('surrogate key');
INSERT INTO column_type (column_type) VALUES ('natural key');
INSERT INTO column_type (column_type) VALUES ('scd type 1');
INSERT INTO column_type (column_type) VALUES ('scd type 2');
INSERT INTO column_type (column_type) VALUES ('scd type 3');
INSERT INTO column_type (column_type) VALUES ('effective start date');
INSERT INTO column_type (column_type) VALUES ('effective end date');
INSERT INTO column_type (column_type) VALUES ('current indicator');
