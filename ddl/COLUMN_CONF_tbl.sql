DROP TABLE column_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE column_conf
       ( owner			VARCHAR2(30) NOT NULL,
	 table_name		VARCHAR2(30) NOT NULL,
	 column_name		VARCHAR2(30) NOT NULL,
	 column_type		VARCHAR2(30) NOT NULL,
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE column_conf IS 'configuration information for column tables for LOAD_DIM procedure';

COMMENT ON COLUMN column_conf.owner IS 'table owner';
COMMENT ON COLUMN column_conf.table_name IS 'name of column table';
COMMENT ON COLUMN column_conf.column_name IS 'name of column associated with the table';
COMMENT ON COLUMN column_conf.column_type IS 'description of the purpose of the column';
COMMENT ON COLUMN column_conf.created_user IS 'for auditing';
COMMENT ON COLUMN column_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN column_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN column_conf.modified_dt IS 'for auditing';

ALTER TABLE column_conf 
      ADD (
	    CONSTRAINT column_conf_pk
	    PRIMARY KEY
	    ( owner, table_name )
	    USING INDEX
	  )
/

ALTER TABLE column_conf
      ADD (
	    CONSTRAINT column_conf_fk1
	    FOREIGN KEY ( column_type )
	    REFERENCES column_type  
	    ( column_type )
	  )
/
