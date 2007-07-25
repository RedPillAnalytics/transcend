SET serveroutput on size unlimited
SET echo off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL CreateTranscendRepository_&_DATE..log

-- first get the schema for the Transcend repository (tables) first
ACCEPT td_rep char default 'TDREP' prompt 'Schema name for the Transcend repository [tdrep]: '

-- install the repository
@@transcend_repository &td_rep

