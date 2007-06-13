CREATE OR REPLACE TYPE extract UNDER fhconf (
   dateformat_ddl   VARCHAR2 (250),
   tsformat_ddl     VARCHAR2 (250),
   delimiter        VARCHAR2 (1),
   quotechar        VARCHAR2 (1),
   headers          VARCHAR2 (3),
   MEMBER FUNCTION extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      VARCHAR2 DEFAULT 'no')
      RETURN NUMBER,
   MEMBER FUNCTION extract_object (
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_append      VARCHAR2 DEFAULT 'no')
      RETURN NUMBER,
   MEMBER PROCEDURE process
)
;
/