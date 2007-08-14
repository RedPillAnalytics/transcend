DROP TABLE index_maint_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE index_maint_conf
       ( owner			VARCHAR2(30) NOT NULL,
	 table_name		VARCHAR2(30) NOT NULL,
	 partname		VARCHAR2(30),
	 source_owner		VARCHAR2(30),
	 source_object		VARCHAR2(30),
	 source_column		VARCHAR2(30),
	 d_num			NUMBER,
	 p_num			NUMBER,
	 index_regexp		VARCHAR2(100),
	 index_type		VARCHAR2(50),
	 part_type		VARCHAR2(50),
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE index_maint_conf IS 'configuration information for index maintenance processes for LOAD_DIM procedure';

COMMENT ON COLUMN index_maint_conf.owner IS 'table owner';
COMMENT ON COLUMN index_maint_conf.table_name IS 'name of table';
COMMENT ON COLUMN index_maint_conf.created_user IS 'for auditing';
COMMENT ON COLUMN index_maint_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN index_maint_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN index_maint_conf.modified_dt IS 'for auditing';

ALTER TABLE index_maint_conf 
      ADD (
	    CONSTRAINT index_maint_conf_pk
	    PRIMARY KEY
	    ( owner, table_name )
	    USING INDEX
	  )
/

ALTER TABLE index_maint_conf
      ADD (
	    CONSTRAINT index_maint_conf_fk1
	    FOREIGN KEY ( owner, table_name )
	    REFERENCES dimension_conf  
	    ( owner, table_name )
	  )
/
