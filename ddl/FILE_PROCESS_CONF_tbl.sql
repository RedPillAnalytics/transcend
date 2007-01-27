DROP TABLE efw.filehub_conf CASCADE CONSTRAINTS purge
/
DROP SEQUENCE efw.filehub_conf_seq
/

CREATE TABLE efw.filehub_conf
       ( filehub_id	NUMBER		NOT NULL,
	 filehub_name	VARCHAR2(100) 	NOT NULL,
	 jobname		VARCHAR2(64) 	NOT NULL,
	 filehub_type	VARCHAR2(7) 	NOT NULL,
	 trg_filename		VARCHAR2(30) 	NOT NULL,
	 object_owner		VARCHAR2(30)    NOT NULL,
	 object_name		VARCHAR2(30)    NOT NULL,
	 dirname        	VARCHAR2(30)    NOT NULL,
	 arch_dirname     	VARCHAR2(30) 	NOT NULL,
	 min_bytes		NUMBER NOT null,
	 max_bytes              NUMBER NOT null,
	 file_timestamp		VARCHAR2(30) 	NOT NULL,
	 created_user   	VARCHAR2(30) 	NOT NULL,
	 created_dt     	DATE 		NOT NULL,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.filehub_conf IS 'table holding configuration information for PROCESSES in the FILE package';

COMMENT ON COLUMN efw.filehub_conf.filehub_id IS 'sequence generated primary key of the table';
COMMENT ON COLUMN efw.filehub_conf.filehub_name IS 'unique name for each distinct process';
COMMENT ON COLUMN efw.filehub_conf.filehub_type IS 'type of filehub, "feed" or "extract"';
COMMENT ON COLUMN efw.filehub_conf.jobname IS 'defines which job (called by whatever scheduling process) owns this process';
COMMENT ON COLUMN efw.filehub_conf.trg_filename IS 'expected filename for the target of the process';
COMMENT ON COLUMN efw.filehub_conf.object_owner IS 'owner of the schema object associated with the file';
COMMENT ON COLUMN efw.filehub_conf.object_name IS 'name of the schema object associated with the file';
COMMENT ON COLUMN efw.filehub_conf.dirname IS 'name of the oracle directory object for the file';
COMMENT ON COLUMN efw.filehub_conf.arch_dirname IS 'name of the oracle directory object for an archive of the file';
COMMENT ON COLUMN efw.filehub_conf.min_bytes IS 'minimum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN efw.filehub_conf.max_bytes IS 'maximum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN efw.filehub_conf.file_timestamp IS 'NLS_TIMESTAMP_FORMAT to use for the timestamp written on the file. A value of NA means that no timestamp will be written on the file.'; 
COMMENT ON COLUMN efw.filehub_conf.created_user IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.modified_dt IS 'for auditing';


ALTER TABLE efw.filehub_conf ADD (
  CONSTRAINT filehub_conf_pk
 PRIMARY KEY
 (filehub_id)
    USING INDEX
    TABLESPACE efw)
/

ALTER TABLE efw.filehub_conf
      ADD CONSTRAINT filehub_conf_uk1 UNIQUE (filehub_name,jobname)
      USING INDEX TABLESPACE efw
/

CREATE SEQUENCE efw.filehub_conf_seq
/
