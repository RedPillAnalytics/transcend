DROP TABLE dir_list purge
/

CREATE global TEMPORARY TABLE dir_list
( filename VARCHAR2(255),
  file_dt date,
  file_size NUMBER
)
ON COMMIT DELETE ROWS
/

GRANT SELECT ON dir_list TO td_sel_&schema
/
GRANT SELECT,UPDATE,DELETE,INSERT ON dir_list TO td_sel_&schema
/