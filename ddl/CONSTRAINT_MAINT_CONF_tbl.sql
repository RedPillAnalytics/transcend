DROP TABLE constraint_maint_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE constraint_maint_conf
       ( owner			VARCHAR2(30) NOT NULL,
	 table_name		VARCHAR2(30) NOT NULL,
	 constraint_type	VARCHAR2(50),
	 constraint_regexp	VARCHAR2(30),
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE constraint_maint_conf IS 'configuration information for constraint maintenance jobs for LOAD_DIM procedure';

COMMENT ON COLUMN constraint_maint_conf.owner IS 'table owner';
COMMENT ON COLUMN constraint_maint_conf.table_name IS 'name of table';
COMMENT ON COLUMN constraint_maint_conf.created_user IS 'for auditing';
COMMENT ON COLUMN constraint_maint_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN constraint_maint_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN constraint_maint_conf.modified_dt IS 'for auditing';

ALTER TABLE constraint_maint_conf 
      ADD (
	    CONSTRAINT constraint_maint_conf_pk
	    PRIMARY KEY
	    ( owner, table_name )
	    USING INDEX
	  )
/