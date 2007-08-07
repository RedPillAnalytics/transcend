select regexp_replace(dbms_metadata.get_ddl('CONSTRAINT',constraint_name,owner),'(\.|constraint +)(")?('|| table_name ||')(\w*)(")?',
		       '\1'||'ar_transaction_stg'||'\4',
                            1,
                            0,
                            'i')
  from dba_constraints 
 where owner='WHDATA'
   AND table_name='AR_TRANSACTION_FACT'
   and constraint_type <> 'R'
/
