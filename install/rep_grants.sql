PROMPT 'Running rep_grants.sql'
-- &1 IS the repository SCHEMA
DEFINE rep_schema_rg = &1

-- create role for this repository
DROP ROLE &rep_schema_rg._sel;
CREATE ROLE &rep_schema_rg._sel;
DROP ROLE &rep_schema_rg._adm;
CREATE ROLE &rep_schema_rg._adm;


GRANT SELECT ON COUNT_TABLE TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON COUNT_TABLE TO &rep_schema_rg._adm;

GRANT SELECT ON DIR_LIST TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON DIR_LIST TO &rep_schema_rg._adm;

GRANT SELECT ON FILES_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON FILES_CONF TO &rep_schema_rg._adm;

GRANT SELECT ON FILES_DETAIL TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON FILES_DETAIL TO &rep_schema_rg._adm;
GRANT SELECT ON files_detail_seq TO &rep_schema_rg._sel;
GRANT SELECT ON files_detail_seq TO &rep_schema_rg._adm;

GRANT SELECT ON FILES_OBJ_DETAIL TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON FILES_OBJ_DETAIL TO &rep_schema_rg._adm;
GRANT SELECT ON files_obj_detail_seq TO &rep_schema_rg._sel;
GRANT SELECT ON files_obj_detail_seq TO &rep_schema_rg._adm;

GRANT SELECT ON LOGGING_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON LOGGING_CONF TO &rep_schema_rg._adm;
GRANT SELECT ON logging_conf_seq TO &rep_schema_rg._sel;
GRANT SELECT ON logging_conf_seq TO &rep_schema_rg._adm;

GRANT SELECT ON LOG_TABLE TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON LOG_TABLE TO &rep_schema_rg._adm;

GRANT SELECT ON NOTIFICATION_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON NOTIFICATION_CONF TO &rep_schema_rg._adm;

GRANT SELECT ON NOTIFICATION_EVENTS TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON NOTIFICATION_EVENTS TO &rep_schema_rg._adm;
GRANT SELECT ON notification_events_seq TO &rep_schema_rg._sel;
GRANT SELECT ON notification_events_seq TO &rep_schema_rg._adm;

GRANT SELECT ON PARAMETER_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON PARAMETER_CONF TO &rep_schema_rg._adm;
GRANT SELECT ON PARAMETER_CONF_SEQ TO &rep_schema_rg._sel;
GRANT SELECT ON PARAMETER_CONF_SEQ TO &rep_schema_rg._adm;

GRANT SELECT ON TD_PART_GTT TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON TD_PART_GTT TO &rep_schema_rg._adm;

GRANT SELECT ON TD_BUILD_IDX_GTT TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON TD_BUILD_IDX_GTT TO &rep_schema_rg._adm;

GRANT SELECT ON TD_BUILD_CON_GTT TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON TD_BUILD_CON_GTT TO &rep_schema_rg._adm;

GRANT SELECT ON TD_CON_MAINT_GTT TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON TD_CON_MAINT_GTT TO &rep_schema_rg._adm;

GRANT SELECT ON DIMENSION_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON DIMENSION_CONF TO &rep_schema_rg._adm;

GRANT SELECT ON COLUMN_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON COLUMN_CONF TO &rep_schema_rg._adm;

GRANT SELECT ON COLUMN_TYPE_LIST TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON COLUMN_TYPE_LIST TO &rep_schema_rg._adm;

GRANT SELECT ON REPLACE_METHOD_LIST TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON REPLACE_METHOD_LIST TO &rep_schema_rg._adm;

GRANT SELECT ON REGISTRATION_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON REGISTRATION_CONF TO &rep_schema_rg._adm;
GRANT SELECT ON REGISTRATION_CONF_SEQ TO &rep_schema_rg._sel;
GRANT SELECT ON REGISTRATION_CONF_SEQ TO &rep_schema_rg._adm;

GRANT SELECT ON RUNMODE_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON RUNMODE_CONF TO &rep_schema_rg._adm;
GRANT SELECT ON RUNMODE_CONF_SEQ TO &rep_schema_rg._sel;
GRANT SELECT ON RUNMODE_CONF_SEQ TO &rep_schema_rg._adm;

GRANT SELECT ON ERROR_CONF TO &rep_schema_rg._sel;
GRANT SELECT,UPDATE,DELETE,INSERT ON ERROR_CONF TO &rep_schema_rg._adm;