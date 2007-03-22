CREATE OR REPLACE TYPE tdinc.EXTRACT UNDER tdinc.fhconf (
   dateformat_ddl   VARCHAR2 (250),
   tsformat_ddl     VARCHAR2 (250),
   delimiter        VARCHAR2 (1),
   quotechar        VARCHAR2 (1),
   headers          VARCHAR2 (1),
   MEMBER FUNCTION extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE)
      RETURN NUMBER,
   MEMBER FUNCTION extract_object (
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'N',
      p_append      BOOLEAN DEFAULT FALSE)
      RETURN NUMBER,
   MEMBER PROCEDURE process_extract
)
;
/