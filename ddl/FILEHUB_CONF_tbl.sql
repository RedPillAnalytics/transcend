DROP TABLE efw.filehub_conf CASCADE CONSTRAINTS purge
/
DROP SEQUENCE efw.filehub_conf_seq
/

CREATE TABLE efw.filehub_conf
       ( filehub_id		NUMBER		NOT NULL DEFAULT filehub_conf_seq.nextval,
	 filehub_name		VARCHAR2(100) 	NOT NULL,
	 filehub_group		VARCHAR2(64) 	NOT NULL,
	 filehub_type		VARCHAR2(7) 	NOT NULL,
	 object_owner		VARCHAR2(30)    NOT NULL,
	 object_name		VARCHAR2(30)    NOT NULL,
	 trg_filename		VARCHAR2(30) 	NOT NULL,
	 dirname        	VARCHAR2(30)    NOT NULL,
	 arch_dirname     	VARCHAR2(30) 	NOT NULL,
	 secondary_dir	 	VARCHAR2(50)	NOT null,
	 min_bytes		NUMBER 		NOT NULL DEFAULT 0,
	 max_bytes              NUMBER 		NOT NULL DEFAULT 0,
	 file_timestamp		VARCHAR2(30) 	NOT NULL DEFAULT 'yyyymmddhhmissxff',
	 dateformat		VARCHAR2(30)   	NOT NULL DEFAULT 'mm/dd/yyyy hh:mi:ss am',
	 delimiter		VARCHAR2(1)    	NOT NULL DEFAULT ',',
	 quotechar		VARCHAR2(1) 	NOT NULL DEFAULT '"',
	 include headers	VARCHAR2(7) 	NOT NULL DEFAULT 
	 multi_files_action	VARCHAR2(10) 	NOT NULL 'newest',
	 file_requirement	VARCHAR2(8) 	NOT NULL 'required',
	 file_notification	VARCHAR2(6) 	NOT NULL 'none',
	 created_user   	VARCHAR2(30) 	NOT NULL sys_context('USERENV','SESSION_USER'),
	 created_dt     	DATE 		DEFAULT sysdate,
	 modified_user  	VARCHAR2(30)	DEFAULT sys_context('USERENV','SESSION_USER'),
	 modified_dt    	DATE		DEFAULT sysdate
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.filehub_conf IS 'table holding configuration information for PROCESSES in the FILE package';

COMMENT ON COLUMN efw.filehub_conf.filehub_id IS 'sequence generated primary key of the table';
COMMENT ON COLUMN efw.filehub_conf.filehub_name IS 'unique name for each distinct process';
COMMENT ON COLUMN efw.filehub_conf.jobname IS 'defines which job (called by whatever scheduling process) owns this process';
COMMENT ON COLUMN efw.filehub_conf.filehub_type IS 'type of filehub, "feed" or "extract"';
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
COMMENT ON COLUMN efw.fh_feed_conf.source_regexp IS 'regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN efw.fh_feed_conf.regexp_ci_ind IS 'indicates whether the REGEXP should be case-insensitive.';
COMMENT ON COLUMN efw.fh_feed_conf.source_dir IS 'name of the directory object where the files are pulled from.';
COMMENT ON COLUMN efw.fh_feed_conf.secondary_dir IS 'A directory to write an exact copy of each file to, in case that functionality is needed';
COMMENT ON COLUMN efw.fh_feed_conf.multi_files_action IS 'Action to take is multiple files match SOURCE_REGEXP. Current options are "newest","oldest","all","fail" or "proceed"';
COMMENT ON COLUMN efw.fh_feed_conf.files_required_ind IS 'A value of "Y" means the job will fail if no files are found.';
COMMENT ON COLUMN efw.fh_extract_conf.dateformat IS 'NLS_DATE_FORMAT of date columns in the extract';
COMMENT ON COLUMN efw.fh_extract_conf.delimiter IS 'delimiter used to separate columns';
COMMENT ON COLUMN efw.fh_extract_conf.quotechar IS 'quotechar used to support columns. An NA specifies that no quotechar is used';
COMMENT ON COLUMN efw.fh_extract_conf.headers IS 'a Y/N indicator of whether headers should be included as the first row in the filw';


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
