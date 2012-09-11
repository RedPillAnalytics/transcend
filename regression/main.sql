
-- Install
@../install/install_transcend
WHENEVER sqlerror exit failure
SET serveroutput off
SET termout off


@setup

-- test Transcend ETL
@dimension1
@fact1

-- test Transcend Files
@file1