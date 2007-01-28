DROP TABLE tdinc.extract_conf CASCADE CONSTRAINTS purge
/
DROP SEQUENCE tdinc.extract_conf_seq
/
CREATE TABLE tdinc.extract_conf
(
  EXTRACT        VARCHAR2(1024)            NOT NULL,
  extract_number NUMBER NOT NULL,
  OBJECT         VARCHAR2(30)              NOT NULL,
  owner          VARCHAR2(30)              NOT NULL,
  filebase       VARCHAR2(30)              NOT NULL,
  filext         VARCHAR2(6)               NOT NULL,
  datestamp      VARCHAR2(30)              NOT NULL,
  DATEFORMAT     VARCHAR2(30)              NOT NULL,
  dirname        VARCHAR2(30)              NOT NULL,
  stgdirname     VARCHAR2(30) NOT NULL,
  delimiter      VARCHAR2(5)               NOT NULL,
  quotechar      VARCHAR2(1) NOT NULL,
  sender         VARCHAR2(1024) NOT NULL,
  recipients     VARCHAR2(2000) NOT NULL,
  baseurl        VARCHAR2(255) NOT NULL,
  headers        VARCHAR2(1) NOT NULL,
  sendmail       VARCHAR2(1) NOT NULL,
  arcdirname     VARCHAR2(30) NOT NULL,
  created_user   VARCHAR2(30) NOT NULL,
  created_dt     DATE NOT NULL,
  modified_user  VARCHAR2(30),
  modified_dt    DATE
)
TABLESPACE tdinc
/

CREATE UNIQUE INDEX tdinc.extract_conf_pk ON tdinc.extract_conf
(EXTRACT)
LOGGING
TABLESPACE tdinc
NOPARALLEL
/
ALTER TABLE tdinc.extract_conf ADD (
  CONSTRAINT extract_conf_pk
 PRIMARY KEY
 (EXTRACT)
    USING INDEX
    TABLESPACE tdinc)
/
CREATE SEQUENCE tdinc.extract_conf_seq
/