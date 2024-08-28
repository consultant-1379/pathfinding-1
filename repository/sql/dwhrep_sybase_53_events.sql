/*=====================================================================================*/
/*Defines the tables for KPI Threshold notification Service */
/*=====================================================================================*/

if(select count(*) from sys.systable where table_name = 'WindowDefinition') = 0
begin
	/*==========================================================================================================*/
	/* Table: WindowDefinition        								                        */
	/* WINDOWID: ID of the window.   						    	                        */
	/* NAME: The name of the window which should mach the name of the corresponding Java class.  	                        */
	/* ROPTYPE: Value should be 1, 5 or 15. Defines the number of minutes for collecting events before checking if the threshold is breached. */
	/*==========================================================================================================*/

	create table WindowDefinition (
		WINDOWID integer not null,
		NAME varchar(50) not null unique,
		ROPTYPE integer not null,
		ENABLED integer not null
	)@system@
	
	alter table WindowDefinition
	   add primary key (WINDOWID)	
	
	/* LTE Attach Susscess Ratio */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 1, 'ImsiGroupAttachSuccessRate4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 2, 'SgsnGroupAttachSuccessRate4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 3, 'PlmnGroupAttachSuccessRate4GWindow', 1, 1)
	
	/* 3G Attach Success Ratio */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 4, 'ImsiGroupAttachSuccessRate3GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 5, 'PlmnGroupAttachSuccessRate3GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 6, 'SgsnGroupAttachSuccessRate3GWindow', 1, 1)

	/* 4G Mobile Service Request */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 7, 'ImsiGroupMobileServiceRequestSR4GWindow', 1, 1)
		
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 8, 'SgsnGroupMobileServiceRequestSR4GWindow', 1, 1)
	
	/* 4G S1 Handover Success Rate */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 9, 'ImsiGroupS1HandoverSuccessRate4GWindow', 1, 1)
		
	/* 3G Bearer Activation */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 10, 'SgsnGroupDedicatedBearerActivationSR4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 11, 'PlmnGroupDedicatedBearerActivationSR4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 12, 'ImsiGroupDedicatedBearerActivationSR4GWindow', 1, 1)

	/* 4G PDN Connection */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 13, 'SgsnGroupPDNConnectionSR4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 14, 'ImsiGroupPDNConnectionSR4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 15, 'PlmnGroupPDNConnectionSR4GWindow', 1, 1)
	
	insert into WindowDefinition
		values ( 16, 'ImsiGroupX2HandoverSR4GWindow', 1, 1)

	/* 4G Network Service Request */
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 17, 'SgsnGroupNetworkServiceRequestSR4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 18, 'ImsiGroupNetworkServiceRequestSR4GWindow', 1, 1)

	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 19, 'PlmnGroupInterTauSR4GWindow', 1, 1)
		
	insert into WindowDefinition (WINDOWID, NAME, ROPTYPE, ENABLED)
		values ( 20, 'PlmnGroupIntraTAUSR4GWindow', 1, 1)
end

if(select count(*) from sys.systable where table_name = 'RuleDefinition') = 0
begin
	/*==========================================================================================================*/
	/* Table: RuleDefinition        								                        */
	/* RULEID: ID of the rule.   						    	                        */
	/* WINDOWID: The windowId that this rule belongs to. */
	/* NAME: The name of the rule which should match the name of the corresponding Java class.  	                        */
	/* DATATYPE: Java type of the field name of the rule. Currently only float is supported. */
	/* FIELDNAME: Name of the field in the Java class  which should be compared with the threshold*/
	/* OPTYPE: The operation that should be used for the comparison, one of: <,>, <=, >=, =, != */
	/* THRESHOLD: The threshold to be compared with the FIELDNAME */
	/*==========================================================================================================*/

	create table RuleDefinition ( 
		RULEID integer not null,
		WINDOWID integer  not null,
		NAME varchar(50) not null unique,
		DATATYPE varchar(10)  not null,
		FIELDNAME varchar(20)  not null,
		OPTYPE varchar(2)  not null,
		THRESHOLD varchar(10)  not null
	)@system@
	
	alter table RuleDefinition
	   add primary key (RULEID)
	   
	/*LTE Attach Success Ratio */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  1, 1, 'ImsiGroupAttachSuccessRate4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  2, 2, 'SgsnGroupAttachSuccessRate4GRule', 'float', 'ratio','<', '98.8')
	
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  3, 3, 'PlmnGroupAttachSuccessRate4GRule', 'float', 'ratio','<', '98.8')

	/*3G Attach Success Ratio */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  4, 4, 'ImsiGroupAttachSuccessRate3GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  5, 5, 'PlmnGroupAttachSuccessRate3GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  6, 6, 'SgsnGroupAttachSuccessRate3GRule', 'float', 'ratio','<', '98.8')

	/*4G Mobile Service Request */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  7, 7, 'ImsiGroupMobileServiceRequestSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  8, 8, 'SgsnGroupMobileServiceRequestSR4GRule', 'float', 'ratio','<', '98.8')

	/*4G S1 Handover Success Rate */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  9, 9, 'ImsiGroupS1HandoverSuccessRate4GRule', 'float', 'ratio','<', '98.8')

	/*3G Bearer Activation */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  10, 10, 'SgsnGroupDedicatedBearerActivationSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  11, 11, 'PlmnGroupDedicatedBearerActivationSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  12, 12, 'ImsiGroupDedicatedBearerActivationSR4GRule', 'float', 'ratio','<', '98.8')

	/*4G PDN Connection */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  13, 13, 'SgsnGroupPDNConnectionSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  14,14, 'ImsiGroupPDNConnectionSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  15,15, 'PlmnGroupPDNConnectionSR4GRule', 'float', 'ratio','<', '98.8')
	
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  16, 16, 'ImsiGroupX2HandoverSR4GRule', 'float', 'ratio','<', '98.8')

	/*4G Network Service Request */
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  17, 17, 'SgsnGroupNetworkServiceRequestSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  18, 18, 'ImsiGroupNetworkServiceRequestSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  19, 19, 'PlmnGroupInterTauSR4GRule', 'float', 'ratio','<', '98.8')

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  20, 20, 'PlmnGroupIntraTAUSR4GRule', 'float', 'ratio','<', '98.8')	
end