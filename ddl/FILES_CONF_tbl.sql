DROP TABLE files_conf CASCADE CONSTRAINTS purge
/
CREATE TABLE files_conf
       ( file_label		VARCHAR2(100) 	NOT NULL,
	 file_group		VARCHAR2(64) 	NOT NULL,
	 file_type		VARCHAR2(7) 	NOT NULL,
	 file_description	VARCHAR2(100),
	 object_owner		VARCHAR2(30)    NOT NULL,
	 object_name		VARCHAR2(30)    NOT NULL,
	 directory		VARCHAR2(30)	NOT NULL,
	 filename		VARCHAR2(50)    NOT NULL,		
	 arch_directory     	VARCHAR2(30) 	NOT NULL,
	 min_bytes		NUMBER 		DEFAULT 0 NOT NULL,
	 max_bytes              NUMBER 		DEFAULT 0 NOT NULL,
	 file_datestamp		VARCHAR2(30),
	 baseurl                VARCHAR2(500),
	 passphrase             VARCHAR2(100),
	 source_directory 	VARCHAR2(50),
	 source_regexp   	VARCHAR2(100),
	 regexp_options		VARCHAR2(10)    DEFAULT 'i',
	 source_policy	        VARCHAR2(10) 	DEFAULT 'newest',
	 required       	VARCHAR2(1) 	DEFAULT 'Y',
	 delete_source 		VARCHAR2(3)     DEFAULT 'Y',
	 reject_limit 		NUMBER,
	 dateformat		VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss am',
	 timestampformat	VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
	 delimiter		VARCHAR2(1)    	DEFAULT ',',
	 quotechar		VARCHAR2(2),
	 headers		VARCHAR2(1),
	 created_user   	VARCHAR2(30),
	 created_dt     	DATE,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE files_conf IS 'table holding configuration information for PROCESSES in the FILE package';

COMMENT ON COLUMN files_conf.file_label IS 'unique name for each distinct file process';
COMMENT ON COLUMN files_conf.file_group IS 'logical grouping of file_labels that can be called together, such as files that are loaded in the same job';
COMMENT ON COLUMN files_conf.file_type IS 'type of file process; "feed" for a file going to an external table; and "extract" for an extract file being generated in a directory.';
COMMENT ON COLUMN files_conf.file_description IS 'a place to describe the file for documentation purposes.';
COMMENT ON COLUMN files_conf.object_owner IS 'owner of the schema object associated with the file';
COMMENT ON COLUMN files_conf.object_name IS 'name of the schema object associated with the file';
COMMENT ON COLUMN files_conf.filename IS 'filename for the target of the process. For extracts, this is the name of the file to output to. For feeds, it is the filename to use in the directory or external table.';
COMMENT ON COLUMN files_conf.arch_directory IS 'name of the oracle directory object for an archive of the file';
COMMENT ON COLUMN files_conf.min_bytes IS 'minimum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN files_conf.max_bytes IS 'maximum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN files_conf.file_datestamp IS 'NLS_DATE_FORMAT to use for the datestamp written on the file. A value of NA means that no timestamp will be written on the file.'; 
COMMENT ON COLUMN files_conf.source_regexp IS 'regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN files_conf.regexp_options IS 'additional match options that can specified in the regular expression';
COMMENT ON COLUMN files_conf.source_directory IS 'name of the directory object where the files are pulled from.';
COMMENT ON COLUMN files_conf.source_policy IS 'Action to take is multiple files match SOURCE_REGEXP. Current options are "newest","oldest","all","fail" or "proceed"';
COMMENT ON COLUMN files_conf.required IS 'Y/N column; determines whether the job fails or not when files are not found.';
COMMENT ON COLUMN files_conf.dateformat IS 'NLS_DATE_FORMAT of date columns in the extract';
COMMENT ON COLUMN files_conf.delimiter IS 'delimiter used to separate columns';
COMMENT ON COLUMN files_conf.quotechar IS 'quotechar used to support columns. A "none" specifies that no quotechar is used';
COMMENT ON COLUMN files_conf.headers IS 'a indicator of whether headers should be included as the first row in the file: "include" or "exclude"';
COMMENT ON COLUMN files_conf.baseurl IS 'the baseurl that the file is located at, which can be included in notifications';
COMMENT ON COLUMN files_conf.created_user IS 'for auditing';
COMMENT ON COLUMN files_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN files_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN files_conf.modified_dt IS 'for auditing';

ALTER TABLE files_conf ADD (
  CONSTRAINT files_conf_pk
 PRIMARY KEY
(file_label, file_group)
    USING INDEX)
/