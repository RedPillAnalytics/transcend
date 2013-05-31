SET autotrace off
SET echo on

-- do some resetting
DROP USER td_demo CASCADE;

create user td_demo identified by password
DEFAULT TABLESPACE users
QUOTA UNLIMITED ON users
/

GRANT CONNECT, RESOURCE TO td_demo
/
grant select any table to td_demo
/

-- test case is reset


-- register any executing user with Transcend
EXEC tdsys.td_adm.register_user('stewart');


-- setup files demo

CREATE OR REPLACE directory td_source AS '/home/oracle/source_files';

CREATE OR REPLACE directory td_files AS '/home/oracle/files';

GRANT READ, WRITE ON directory td_source TO stewart;

GRANT READ, WRITE ON directory td_files TO stewart;


-- register directories with Transcend
-- this is required because Java Stored Procedures are used
-- this handles setting Java permissions on the directory

EXEC tdsys.td_adm.register_directory('td_source','tdrep','stewart');

EXEC tdsys.td_adm.register_directory('td_files','tdrep','stewart');

PURGE recyclebin;