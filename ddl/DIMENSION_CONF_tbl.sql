DROP TABLE dimension_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE dimension_conf
       ( owner			VARCHAR2(30) NOT NULL,
	 table_name		VARCHAR2(30) NOT NULL,
	 source_owner		VARCHAR2(30) NOT NULL,
	 source_object		VARCHAR2(30) NOT NULL,
	 sequence_owner  	VARCHAR2(30) NOT NULL,
	 sequence_name  	VARCHAR2(30) NOT NULL,
	 staging_owner		VARCHAR2(30),
	 staging_table		VARCHAR2(30),
	 direct_load		VARCHAR2(3) DEFAULT 'yes' NOT NULL,
	 replace_method		VARCHAR2(10) DEFAULT 'rename' NOT NULL,
	 statistics		VARCHAR2(10),
	 created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE dimension_conf IS 'configuration information for dimension tables for LOAD_DIM procedure';

COMMENT ON COLUMN dimension_conf.owner IS 'table owner';
COMMENT ON COLUMN dimension_conf.table_name IS 'name of dimension table';
COMMENT ON COLUMN dimension_conf.source_owner IS 'owner of source_object';
COMMENT ON COLUMN dimension_conf.source_object IS 'object--table or view--containing records going into dimension table for the day';
COMMENT ON COLUMN dimension_conf.staging_owner IS 'owner of staging_table';
COMMENT ON COLUMN dimension_conf.staging_table IS 'table used for a landing point for the analytics query building the dimension table';
COMMENT ON COLUMN dimension_conf.sequence_owner IS 'owner of the sequence used for the surrogate key';
COMMENT ON COLUMN dimension_conf.sequence_name IS 'name of the sequence used for the surrogate key';
COMMENT ON COLUMN dimension_conf.replace_method IS 'method for loading the final data set back into the dimension table';
COMMENT ON COLUMN dimension_conf.created_user IS 'for auditing';
COMMENT ON COLUMN dimension_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN dimension_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN dimension_conf.modified_dt IS 'for auditing';

ALTER TABLE dimension_conf 
      ADD (
	    CONSTRAINT dimension_conf_pk
	    PRIMARY KEY
	    ( owner, table_name )
	    USING INDEX
	  )
/

ALTER TABLE dimension_conf
      ADD (
	    CONSTRAINT dimension_conf_fk1
	    FOREIGN KEY ( replace_method )
	    REFERENCES replace_method_list
	    ( replace_method )
	  )
/

-- constraint ensures business logic regarding replace_method
ALTER TABLE dimension_conf
      ADD CONSTRAINT replace_method_ck
      CHECK ( upper(staging_owner) = CASE WHEN replace_method = 'rename' THEN upper(owner) ELSE staging_owner end )
/