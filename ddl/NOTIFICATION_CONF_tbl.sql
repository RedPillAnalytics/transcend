DROP TABLE notification_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE notification_conf
       ( label		   VARCHAR2(40) NOT NULL,
	 module        	   VARCHAR2(48) NOT NULL,
	 action        	   VARCHAR2(32) NOT NULL,
	 method      	   VARCHAR2(20) NOT NULL,
	 enabled     	   VARCHAR2(3) DEFAULT 'yes',
	 required	   VARCHAR2(3) DEFAULT 'no',
	 sender            VARCHAR2(1024),
	 recipients        VARCHAR2(2000) NOT NULL,
	 created_user      VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	 created_dt   	   DATE DEFAULT SYSDATE NOT NULL,
	 modified_user     VARCHAR2(30),
	 modified_dt       DATE
       )
/

ALTER TABLE notification_conf ADD
      (
	CONSTRAINT notification_conf_pk
	PRIMARY KEY
	( notification_label,module,action )
	USING INDEX
      )
/
ALTER TABLE notification_conf
      ADD (
       CONSTRAINT notification_conf_fk1
       FOREIGN KEY ( module, action )
       REFERENCES notification_events
       ( module, action )
     )
/