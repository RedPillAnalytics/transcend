DROP TABLE efw.file_process_conf CASCADE CONSTRAINTS purge
/
DROP SEQUENCE efw.file_process_conf_seq
/
CREATE TABLE efw.file_process_conf
       ( file_process_id NUMBER NOT NULL,
	 file_process_name VARCHAR2(100) NOT NULL,
	 jobname VARCHAR2(64) NOT NULL,
	 object_owner	VARCHAR2(30)              NOT NULL,
	 object_name     VARCHAR2(30)              NOT NULL,
	 dirname        VARCHAR2(30)              NOT NULL,
	 arcdirname     VARCHAR2(30) NOT NULL,
	 created_user   VARCHAR2(30) NOT NULL,
	 created_dt     DATE NOT NULL,
	 modified_user  VARCHAR2(30),
	 modified_dt    DATE
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.file_process_conf IS 'table holding configuration information for PROCESSES in the FILE package';

COMMENT ON COLUMN efw.file_process_conf.file_process_id IS 'sequence generated primary key of the table';
COMMENT ON COLUMN efw.file_process_conf.file_process_name IS 'unique name for each distinct process';
COMMENT ON COLUMN efw.file_process_conf.jobname IS 'defines which job (called by whatever scheduling process) owns this process';
COMMENT ON COLUMN efw.file_process_conf.object_owner IS 'owner of the schema object associated with the file';
COMMENT ON COLUMN efw.file_process_conf.object_name IS 'name of the schema object associated with the file';
COMMENT ON COLUMN efw.file_process_conf.dirname IS 'name of the oracle directory object for the file';
COMMENT ON COLUMN efw.file_process_conf.arcdirname IS 'name of the oracle directory object for an archive of the file';
COMMENT ON COLUMN efw.file_process_conf.created_user IS 'For Auditing';
COMMENT ON COLUMN efw.file_process_conf.created_dt IS 'For Auditing';
COMMENT ON COLUMN efw.file_process_conf.modified_user IS 'For Auditing';
COMMENT ON COLUMN efw.file_process_conf.modified_dt IS 'For Auditing';


ALTER TABLE efw.file_process_conf ADD (
  CONSTRAINT file_process_conf_pk
 PRIMARY KEY
 (file_process_id)
    USING INDEX
    TABLESPACE efw)
/

CREATE SEQUENCE efw.file_process_conf_seq
/
