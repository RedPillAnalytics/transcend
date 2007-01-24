DROP TABLE efw.file_feed_conf purge
/

CREATE TABLE efw.file_feed_conf
       ( file_process_id	NUMBER NOT NULL,
	 source_regexp   	VARCHAR2(100) NOT NULL,
	 regexp_ci_ind		VARCHAR2(1) NOT NULL,
	 source_dir	 	VARCHAR2(30) NOT NULL,
	 min_bytes		NUMBER NOT null,
	 max_bytes              NUMBER NOT null,
	 secondary_dir	 	VARCHAR2(50) NOT null,
	 multi_files_action	VARCHAR2(10) NOT null,
	 files_required_ind	VARCHAR2(1) NOT NULL,	 
	 created_user    	VARCHAR2(30) NOT null,
	 created_dt      	DATE NOT null,
	 modified_user   	VARCHAR2(30),
	 modified_dt     	DATE
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.file_feed_conf IS 'Configuration portition of the File Mover process. The table should only be updated through ETLFW.FILE_MOVE.REGISTER_JOB_FILES.'
/

COMMENT ON COLUMN efw.file_feed_conf.file_process_id IS 'the same value as the FILE_PROCESS_CONF; also the primary key';
COMMENT ON COLUMN efw.file_feed_conf.source_regexp IS 'regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN efw.file_feed_conf.regexp_ci_ind IS 'indicates whether the REGEXP should be case-insensitive.';
COMMENT ON COLUMN efw.file_feed_conf.source_dir IS 'name of the directory object where the files are pulled from.';
COMMENT ON COLUMN efw.file_feed_conf.min_bytes IS 'minimum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN efw.file_feed_conf.max_bytes IS 'maximum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN efw.file_feed_conf.secondary_dir IS 'A directory to write an exact copy of each file to, in case that functionality is needed';
COMMENT ON COLUMN efw.file_feed_conf.ext_filename IS 'The name of the file the external table is expecting.';
COMMENT ON COLUMN efw.file_feed_conf.multi_files_action IS 'Action to take is multiple files match SOURCE_REGEXP.';
COMMENT ON COLUMN efw.file_feed_conf.files_required_ind IS 'A value of "Y" means the job will fail if no files are found.';
COMMENT ON COLUMN efw.file_feed_conf.created_user IS 'for auditing';
COMMENT ON COLUMN efw.file_feed_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN efw.file_feed_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN efw.file_feed_conf.modified_dt IS 'for auditing';


ALTER TABLE efw.file_feed_conf
      ADD CONSTRAINT file_ctl_pk PRIMARY KEY (file_process_id)
      USING INDEX TABLESPACE efw
/

ALTER TABLE efw.file_feed_conf
      ADD CONSTRAINT file_ctl_uk2 UNIQUE (source_regexp)
      USING INDEX TABLESPACE efw
/

DROP SEQUENCE efw.file_feed_conf_seq
/

CREATE SEQUENCE efw.file_feed_conf_seq START WITH 120
/

GRANT SELECT ON efw.file_feed_conf TO efw_filemover
/
