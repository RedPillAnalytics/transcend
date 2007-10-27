PROMPT 'Running transcend_application.sql'
-- &1 IS the application SCHEMA
DEFINE app_schema_ta = &1
-- &2 IS the DEFAULT repository SCHEMA
DEFINE rep_schema_ta = &2
-- first create the user if it doesn't already exist
@create_app_user &app_schema_ta

-- grant privileges required for package to compile
-- also privileges needed for the application schema to operate with full power
@full_app_grants &app_schema_ta

-- set current schema
ALTER SESSION SET current_schema=&app_schema_ta;

--CREATE java stored procedure
@../java/TdCore.jvs

--DROP all types due to inheritance
DROP _OT dimension_ot;
DROP _OT feed_ot;
DROP _OT extract_ot;
DROP _OT file_ot;
DROP _OT email_ot;
DROP _OT notify_ot;
DROP _OT td_ot;
DROP _OT app_ot;

--CREATE core pieces
@../plsql/specs/STRING_AGG_OT.tps
@../plsql/wrapped_bodies/STRING_AGG_OT.plb
@../plsql/wrapped_bodies/STRAGG.plb
@../plsql/specs/TD_EXT.pks
@../plsql/wrapped_bodies/TD_EXT.plb
@../plsql/specs/TD_INST.pks
@../plsql/wrapped_bodies/TD_INST.plb

--CREATE targeted _ots, packages and object views
@../plsql/specs/APP_OT.tps
@../plsql/wrapped_bodies/APP_OT.plb
@../plsql/specs/NOTIFY_OT.tps
@../plsql/specs/EMAIL_OT.tps
@../plsql/wrapped_bodies/EMAIL_OT.plb
@../object_views/EMAIL_OT_vw.sql
@../plsql/specs/EVOLVE_OT.tps
@../plsql/wrapped_bodies/EVOLVE_OT.plb
@../plsql/specs/TD_SQL.pks
@../plsql/wrapped_bodies/TD_SQL.plb
@../plsql/specs/TD_HOST.pks
@../plsql/wrapped_bodies/TD_HOST.plb
@../plsql/specs/TD_DDL.pks
@../plsql/wrapped_bodies/TD_DDL.plb
@../plsql/specs/FILE_OT.tps
@../plsql/wrapped_bodies/FILE_OT.plb
@../plsql/specs/EXTRACT_OT.tps
@../plsql/wrapped_bodies/EXTRACT_OT.plb
@../object_views/EXTRACT_OT_vw.sql
@../plsql/specs/FEED_OT.tps
@../plsql/wrapped_bodies/FEED_OT.plb
@../object_views/FEED_OT_vw.sql
@../plsql/specs/DIMENSION_OT.tps
--@../plsql/wrapped_bodies/DIMENSION_OT.plb
@../object_views/DIMENSION_OT_vw.sql

--CREATE callable packages
@../plsql/specs/TD_ETL.pks
@../plsql/wrapped_bodies/TD_ETL.plb
@../plsql/specs/TD_FILES.pks
@../plsql/wrapped_bodies/TD_FILES.plb
@../plsql/specs/TD_CONTROL.pks
@../plsql/wrapped_bodies/TD_CONTROL.plb
@../plsql/specs/TD_OWBAPI.pks
@../plsql/wrapped_bodies/TD_OWBAPI.plb

-- create role to execute this application
-- grant all the needed privileges to the role
@exec_app_grants &app_schema_ta

-- write application tracking record
BEGIN
   UPDATE tdsys.applications
      SET repository_name = upper('&rep_schema_ta'),
          modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
          modified_dt = SYSDATE
    WHERE application_name=upper('&app_schema_ta');

   IF SQL%ROWCOUNT = 0
   THEN
      INSERT INTO tdsys.applications
	     ( application_name,
	       repository_name)
	     VALUES
	     ( upper('&app_schema_ta'),
	       upper('&rep_schema_ta'));
   END IF;
END;
/

-- go back to connected user as current_schema
ALTER SESSION SET current_schema=&_USER;