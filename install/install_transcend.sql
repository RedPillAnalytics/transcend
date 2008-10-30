@install_evolve.sql

PROMPT 'Running install_transcend.sql'
DEFINE suffix = _&_DATE..log
SPOOL InstallTranscend&suffix

DECLARE
   l_drop BOOLEAN := CASE WHEN REGEXP_LIKE('yes','&drop_repo','i') THEN TRUE ELSE FALSE END;
BEGIN
   -- create the Transcend repository
   tdsys.td_adm.build_repository( p_schema => '&rep_schema', p_product => tdsys.td_adm.transcend_product, p_tablespace => '&tablespace', p_drop => l_drop );
   -- create the Trancend application
   tdsys.td_adm.build_application( p_schema => '&app_schema', p_product => tdsys.td_adm.transcend_product, p_repository => '&rep_schema' );
EXCEPTION
   WHEN tdsys.td_adm.repo_obj_exists
   THEN
   raise_application_error(-20003,'Repository tables exist. Specify ''Y'' when prompted to issue DROP TABLE statements');
END;
/

-- Install the Transcend Pieces

--CREATE targeted _ots, packages and object views
@../transcend/plsql/specs/TD_DBUTILS.pks
@../transcend/plsql/wrapped_bodies/TD_DBUTILS.plb
@../transcend/plsql/specs/FILE_LABEL_OT.tps
@../transcend/plsql/wrapped_bodies/FILE_LABEL_OT.plb
@../transcend/plsql/specs/FILE_DETAIL_OT.tps
@../transcend/plsql/wrapped_bodies/FILE_DETAIL_OT.plb
@../transcend/plsql/specs/EXTRACT_OT.tps
@../transcend/plsql/wrapped_bodies/EXTRACT_OT.plb
@../transcend/plsql/specs/FEED_OT.tps
@../transcend/plsql/wrapped_bodies/FEED_OT.plb
@../transcend/plsql/specs/MAPPING_OT.tps
@../transcend/plsql/wrapped_bodies/MAPPING_OT.plb
@../transcend/plsql/specs/DIMENSION_OT.tps
@../transcend/plsql/wrapped_bodies/DIMENSION_OT.plb

-- CREATE factory package
@../transcend/plsql/specs/TRANS_FACTORY.pks
@../transcend/plsql/wrapped_bodies/TRANS_FACTORY.plb

--CREATE callable packages
@../transcend/plsql/specs/TRANS_ADM.pks
@../transcend/plsql/wrapped_bodies/TRANS_ADM.plb
@../transcend/plsql/specs/TRANS_ETL.pks
@../transcend/plsql/wrapped_bodies/TRANS_ETL.plb
@../transcend/plsql/specs/TRANS_FILES.pks
@../transcend/plsql/wrapped_bodies/TRANS_FILES.plb

-- set Evolve configurations specific to Transcend
EXEC trans_adm.set_default_configs;

SPOOL off