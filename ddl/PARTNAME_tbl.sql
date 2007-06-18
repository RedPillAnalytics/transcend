DROP TABLE partname purge
/

CREATE global TEMPORARY TABLE partname
( partition_name VARCHAR2(30)
)
ON COMMIT DELETE ROWS
/
