CREATE OR REPLACE PACKAGE efw.extracts
AS
   PROCEDURE register_extract (
      p_extract      gen_extract_conf.EXTRACT%TYPE,
                               -- The name of the report to generate. This is the PK for the table.
      p_object       gen_extract_conf.OBJECT%TYPE DEFAULT NULL,
                                   -- The name of the object to extract: a table or view typically.
      p_owner        gen_extract_conf.owner%TYPE DEFAULT NULL,          -- The owner of the object.
      p_filebase     gen_extract_conf.filebase%TYPE DEFAULT NULL,
                         -- Basename of the extract file... minus the datastamp and file extension.
      p_filext       gen_extract_conf.filext%TYPE DEFAULT NULL,
                                                         -- Extension to place at the end of a file
      p_datestamp    gen_extract_conf.datestamp%TYPE DEFAULT NULL,
                                                          -- NLS_DATE_FORMAT for the file datestamp
      p_dateformat   gen_extract_conf.DATEFORMAT%TYPE DEFAULT NULL,
                                                -- NLS_DATE_FORMAT for any date columns in the file
      p_dirname      gen_extract_conf.dirname%TYPE DEFAULT NULL,
                              -- Name of the Oracle directory object to stage the file in initially
      p_stgdirname   gen_extract_conf.stgdirname%TYPE DEFAULT NULL,
                                              -- Name of the Oracle directory object to extract to.
      p_delimiter    gen_extract_conf.delimiter%TYPE DEFAULT NULL,
                                                           -- Column delimiter in the extract file.
      p_quotechar    gen_extract_conf.quotechar%TYPE DEFAULT NULL,
                                                     -- Character (if any) to use to quote columns.
      p_sender       gen_extract_conf.sender%TYPE DEFAULT NULL,
                                                     -- Character (if any) to use to quote columns.
      p_recipients   gen_extract_conf.recipients%TYPE DEFAULT NULL,
                                                              -- comma separated list of recipients
      p_baseurl      gen_extract_conf.baseurl%TYPE DEFAULT NULL,
                                                    -- URL (minus filename) of the link to the file
      p_headers      gen_extract_conf.headers%TYPE DEFAULT NULL,
                                                          -- whether to include headers in the file
      p_sendmail     gen_extract_conf.sendmail%TYPE DEFAULT NULL,
                                                    -- whether to send an email announcing the link
      p_arcdirname   gen_extract_conf.arcdirname%TYPE DEFAULT NULL,
      p_debug        BOOLEAN DEFAULT FALSE);

   PROCEDURE extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE);

   PROCEDURE extract_object (
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE,
      p_headers     BOOLEAN DEFAULT FALSE);

   FUNCTION get_numlines
      RETURN NUMBER;

   PROCEDURE extract_regexp (
      p_owner       VARCHAR2,
      p_regexp      VARCHAR2,
      p_filext      VARCHAR2 DEFAULT '.csv',
      p_dirname     VARCHAR2 DEFAULT 'MAIL_DIR',
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '"');

   PROCEDURE gen_extract (
      p_extract      gen_extract_conf.EXTRACT%TYPE,
                               -- The name of the report to generate. This is the PK for the table.
      p_object       gen_extract_conf.OBJECT%TYPE DEFAULT NULL,
                                   -- The name of the object to extract: a table or view typically.
      p_owner        gen_extract_conf.owner%TYPE DEFAULT NULL,          -- The owner of the object.
      p_filebase     gen_extract_conf.filebase%TYPE DEFAULT NULL,
                         -- Basename of the extract file... minus the datastamp and file extension.
      p_filext       gen_extract_conf.filext%TYPE DEFAULT NULL,
                                                         -- Extension to place at the end of a file
      p_datestamp    gen_extract_conf.datestamp%TYPE DEFAULT NULL,
                                                          -- NLS_DATE_FORMAT for the file datestamp
      p_dateformat   gen_extract_conf.DATEFORMAT%TYPE DEFAULT NULL,
                                                -- NLS_DATE_FORMAT for any date columns in the file
      p_dirname      gen_extract_conf.dirname%TYPE DEFAULT NULL,
                              -- Name of the Oracle directory object to stage the file in initially
      p_stgdirname   gen_extract_conf.stgdirname%TYPE DEFAULT NULL,
                                              -- Name of the Oracle directory object to extract to.
      p_delimiter    gen_extract_conf.delimiter%TYPE DEFAULT NULL,
                                                           -- Column delimiter in the extract file.
      p_quotechar    gen_extract_conf.quotechar%TYPE DEFAULT NULL,
                                                     -- Character (if any) to use to quote columns.
      p_recipients   gen_extract_conf.recipients%TYPE DEFAULT NULL,
                                                              -- comma separated list of recipients
      p_baseurl      gen_extract_conf.baseurl%TYPE DEFAULT NULL,
                                                    -- URL (minus filename) of the link to the file
      p_headers      BOOLEAN DEFAULT NULL,                -- whether to include headers in the file
      p_sendmail     BOOLEAN DEFAULT NULL,          -- whether to send an email announcing the link
      p_arcdirname   gen_extract_conf.arcdirname%TYPE DEFAULT NULL,
      p_debug        BOOLEAN DEFAULT FALSE);
END extracts;
/