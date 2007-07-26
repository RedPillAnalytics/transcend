PROMPT 'Running CreateTranscendRepository.sql'
SET serveroutput on size unlimited
SET echo off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL CreateTranscendRepository_&_DATE..log

-- first get the schema for the Transcend repository (tables) first
ACCEPT td_rep char default 'TDREP' prompt 'Schema name for the Transcend repository [tdrep]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDREP' prompt 'Tablespace in which to install Transcend default repository: [tdrep]: '

-- get the current default tablespace of the repository user
VARIABLE old_tbspace char(30)
BEGIN
   SELECT default_tablespace
     INTO :old_tbspace
     FROM dba_users
    WHERE username=upper('&td_rep');
END;
/

-- alter the default tablespace of the repository user
ALTER USER &td_rep DEFAULT TABLESPACE &tablespace;
ALTER USER &td_rep QUOTA 50M ON &TABLESPACE;


-- install the repository
@@transcend_repository &td_rep
