CREATE OR REPLACE PACKAGE efw.file_extract
AS
   PROCEDURE register_extract(
      p_extract_number   extract_conf.extract_number%TYPE DEFAULT NULL,
      p_extract          extract_conf.EXTRACT%TYPE,
      -- The name of the report to generate. This is the PK for the table.
      p_object           extract_conf.OBJECT%TYPE DEFAULT NULL,
      -- The name of the object to extract: a table or view typically.
      p_owner            extract_conf.owner%TYPE DEFAULT NULL,          -- The owner of the object.
      p_filebase         extract_conf.filebase%TYPE DEFAULT NULL,
      -- Basename of the extract file... minus the datastamp and file extension.
      p_filext           extract_conf.filext%TYPE DEFAULT NULL,
      -- Extension to place at the end of a file
      p_datestamp        extract_conf.datestamp%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for the file datestamp
      p_dateformat       extract_conf.DATEFORMAT%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for any date columns in the file
      p_dirname          extract_conf.dirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to stage the file in initially
      p_stgdirname       extract_conf.stgdirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to extract to.
      p_delimiter        extract_conf.delimiter%TYPE DEFAULT NULL,
      -- Column delimiter in the extract file.
      p_quotechar        extract_conf.quotechar%TYPE DEFAULT NULL,
      -- Character (if any) to use to quote columns.
      p_sender           extract_conf.sender%TYPE DEFAULT NULL,
      -- Character (if any) to use to quote columns.
      p_recipients       extract_conf.recipients%TYPE DEFAULT NULL,
      -- comma separated list of recipients
      p_baseurl          extract_conf.baseurl%TYPE DEFAULT NULL,
      -- URL (minus filename) of the link to the file
      p_headers          extract_conf.headers%TYPE DEFAULT NULL,
      -- whether to include headers in the file
      p_sendmail         extract_conf.sendmail%TYPE DEFAULT NULL,
      -- whether to send an email announcing the link
      p_arcdirname       extract_conf.arcdirname%TYPE DEFAULT NULL,
      p_debug            BOOLEAN DEFAULT FALSE );

   PROCEDURE extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE );

   PROCEDURE extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE,
      p_headers     BOOLEAN DEFAULT FALSE );

   FUNCTION get_numlines
      RETURN NUMBER;

   PROCEDURE extract_regexp(
      p_owner       VARCHAR2,
      p_regexp      VARCHAR2,
      p_filext      VARCHAR2 DEFAULT '.csv',
      p_dirname     VARCHAR2 DEFAULT 'MAIL_DIR',
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '"' );

   PROCEDURE process_extract(
      p_extract      extract_conf.EXTRACT%TYPE,
      -- The name of the report to generate. This is the PK for the table.
      p_object       extract_conf.OBJECT%TYPE DEFAULT NULL,
      -- The name of the object to extract: a table or view typically.
      p_owner        extract_conf.owner%TYPE DEFAULT NULL,              -- The owner of the object.
      p_filebase     extract_conf.filebase%TYPE DEFAULT NULL,
      -- Basename of the extract file... minus the datastamp and file extension.
      p_filext       extract_conf.filext%TYPE DEFAULT NULL,
      -- Extension to place at the end of a file
      p_datestamp    extract_conf.datestamp%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for the file datestamp
      p_dateformat   extract_conf.DATEFORMAT%TYPE DEFAULT NULL,
      -- NLS_DATE_FORMAT for any date columns in the file
      p_dirname      extract_conf.dirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to stage the file in initially
      p_stgdirname   extract_conf.stgdirname%TYPE DEFAULT NULL,
      -- Name of the Oracle directory object to extract to.
      p_delimiter    extract_conf.delimiter%TYPE DEFAULT NULL,
      -- Column delimiter in the extract file.
      p_quotechar    extract_conf.quotechar%TYPE DEFAULT NULL,
      -- Character (if any) to use to quote columns.
      p_recipients   extract_conf.recipients%TYPE DEFAULT NULL,
      -- comma separated list of recipients
      p_baseurl      extract_conf.baseurl%TYPE DEFAULT NULL,
      -- URL (minus filename) of the link to the file
      p_headers      BOOLEAN DEFAULT NULL,                -- whether to include headers in the file
      p_sendmail     BOOLEAN DEFAULT NULL,          -- whether to send an email announcing the link
      p_arcdirname   extract_conf.arcdirname%TYPE DEFAULT NULL,
      p_debug        BOOLEAN DEFAULT FALSE );
END file_extract;
/