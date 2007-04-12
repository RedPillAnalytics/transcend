DROP TABLE tdinc.partname purge
/

CREATE global TEMPORARY TABLE tdinc.partname
( partition_name VARCHAR2(30)
)
ON COMMIT DELETE ROWS
/
