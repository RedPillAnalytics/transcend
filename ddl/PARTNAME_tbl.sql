DROP TABLE partname purge
/

CREATE global TEMPORARY TABLE partname
( table_owner VARCHAR2(30),
  table_name VARCHAR2(30),
  partition_name VARCHAR2(30),
  partition_position NUMBER
)
ON COMMIT DELETE ROWS
/
