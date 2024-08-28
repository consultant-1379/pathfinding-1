/*=====================================================================================*/
/*Defines the tables for KPI Threshold notification Service */
/*=====================================================================================*/

if(select count(*) from sys.systable where table_name = 'WindowDefinition') = 1
begin
	/*==========================================================================================================*/
	/* Table: WindowDefinition        								                        */
	/* WINDOWID: ID of the window.   						    	                        */
	/* NAME: The name of the window which should mach the name of the corresponding Java class.  	                        */
	/* ROPTYPE: Value should be 1, 5 or 15. Defines the number of minutes for collecting events before checking if the threshold is breached. */
	/*==========================================================================================================*/

	ALTER TABLE WindowDefinition ADD KPINAME varchar(100) null 
	ALTER TABLE WindowDefinition ADD SHORTKPINAME varchar(50) null
    ALTER TABLE WindowDefinition ADD KPIFORMULA varchar(100)  null
    ALTER TABLE windowdefinition ADD THRESHOLDSEVERITYVALUE float default 1.5
	
	
	UPDATE WindowDefinition SET  KPINAME = 'LTE Attach Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='LAttachSR', KPIFORMULA = '100[%]*#SuccessAttachEvents/#TotalAttachEvents' WHERE WINDOWID = 1

	UPDATE WindowDefinition SET  KPINAME = 'LTE Attach Success Ratio - SGSN-MME group', SHORTKPINAME ='LAttachSR', KPIFORMULA = '100[%]*#SuccessAttachEvents/#TotalAttachEvents' WHERE WINDOWID = 2

	UPDATE WindowDefinition SET  KPINAME = 'LTE Attach Success Ratio - PLMN group', SHORTKPINAME ='LAttachSR', KPIFORMULA = '100[%]*#SuccessAttachEvents/#TotalAttachEvents' WHERE WINDOWID = 3

	UPDATE WindowDefinition SET  KPINAME = 'Attach Success Rate - Subscriber (IMSI) group', SHORTKPINAME ='AttachSR', KPIFORMULA = '100[%]*#SuccessAttachEvents/#TotalAttachEvents' WHERE WINDOWID = 4

	UPDATE WindowDefinition SET  KPINAME = 'Attach Success Rate - PLMN group', SHORTKPINAME ='AttachSR', KPIFORMULA = '100[%]*#SuccessAttachEvents/#TotalAttachEvents' WHERE WINDOWID = 5

	UPDATE WindowDefinition SET  KPINAME = 'Attach Success Rate - SGSN-MME group', SHORTKPINAME ='AttachSR', KPIFORMULA = '100[%]*#SuccessAttachEvents/#TotalAttachEvents' WHERE WINDOWID = 6

	UPDATE WindowDefinition SET  KPINAME = 'LTE MS initiated Service Request Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='LMSSRVREQSR', KPIFORMULA = '100[%]*#SuccessfulUEInitiatedServiceRequestEvents/#TotalUEInitiatedServiceRequestEvents' WHERE WINDOWID = 7
		
	UPDATE WindowDefinition SET  KPINAME = 'LTE MS initiated Service Request Success Ratio - SGSN-MME group', SHORTKPINAME ='LMSSRVREQSR', KPIFORMULA = '100[%]*#SuccessfulUEInitiatedServiceRequestEvents/#TotalUEInitiatedServiceRequestEvents' WHERE WINDOWID = 8

	UPDATE WindowDefinition SET  KPINAME = 'S1 Handover Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='S1HOSR', KPIFORMULA = '100[%]*#SuccessS1HandoverEvents/#TotalS1HandoverEvents' WHERE WINDOWID = 9
		
	UPDATE WindowDefinition SET  KPINAME = 'Dedicated Bearer Activation Success Ratio - SGSN-MME group', SHORTKPINAME ='BrActSR', KPIFORMULA = '100[%]*#SuccessDedicatedBearerActivateEvent/#TotalDedicatedBearerActivateEvents' WHERE WINDOWID = 10

	UPDATE WindowDefinition SET  KPINAME = 'Dedicated Bearer Activation Success Ratio - PLMN group', SHORTKPINAME ='BrActSR', KPIFORMULA = '100[%]*#SuccessDedicatedBearerActivateEvent/#TotalDedicatedBearerActivateEvents' WHERE WINDOWID = 11

	UPDATE WindowDefinition SET  KPINAME = 'Dedicated Bearer Activation Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='BrActSR', KPIFORMULA = '100[%]*#SuccessDedicatedBearerActivateEvent/#TotalDedicatedBearerActivateEvents' WHERE WINDOWID = 12

	UPDATE WindowDefinition SET  KPINAME = 'PDN Connection Success Ratio - SGSN-MME group', SHORTKPINAME ='PDNConnSR', KPIFORMULA = '100[%]*#SuccessPDNConnectEvents/#TotalPDNConnectEvents' WHERE WINDOWID = 13

	UPDATE WindowDefinition SET  KPINAME = 'PDN Connection Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='PDNConnSR', KPIFORMULA = '100[%]*#SuccessPDNConnectEvents/#TotalPDNConnectEvents' WHERE WINDOWID = 14

	UPDATE WindowDefinition SET  KPINAME = 'PDN Connection Success Ratio - PLMN group', SHORTKPINAME ='PDNConnSR', KPIFORMULA = '100[%]*#SuccessPDNConnectEvents/#TotalPDNConnectEvents' WHERE WINDOWID = 15

	UPDATE WindowDefinition SET  KPINAME = 'X2 Handover Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='X2HOSR', KPIFORMULA = '100[%]*#SuccessX2HandoverEvents/#TotalX2HandoverEvents' WHERE WINDOWID = 16

	UPDATE WindowDefinition SET  KPINAME = 'LTE network initiated Service Request Success Ratio - SGSN-MME group', SHORTKPINAME ='LNWKSRVREQSR', KPIFORMULA = '100[%]*#SuccessfulPagingEvents/#TotalPagingEvents' WHERE WINDOWID = 17

	UPDATE WindowDefinition SET  KPINAME = 'LTE network initiated Service Request Success Ratio - Subscriber (IMSI) group', SHORTKPINAME ='LNWKSRVREQSR', KPIFORMULA = '100[%]*#SuccessfulPagingEvents/#TotalPagingEvents' WHERE WINDOWID = 18

	UPDATE WindowDefinition SET  KPINAME = 'Inter MME Tracking Area Update Success Ratio - PLMN group', SHORTKPINAME ='IMTAUSR', KPIFORMULA = '100[%]*#SuccessInterTAUEvents/#TotalInterTAUEvents' WHERE WINDOWID = 19
		
	UPDATE WindowDefinition SET  KPINAME = 'Intra MME Tracking Area Update Success Ratio - PLMN group', SHORTKPINAME ='TAUSR', KPIFORMULA = '100[%]*#SuccessIntraTAUEvents/#TotalIntraTAUEvents' WHERE WINDOWID = 20
		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 21, 'ImsiGroupMsPDPContextActivateSR3GWindow', 'MS initiated PDP Context Activation Success Ratio - Subscriber (IMSI)', 1, 1, '100[%]*#SuccessPDPActivationEvents/#TotalPDPActivationEvents', 'MSPDPActSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 22, 'ImsiGroupNetworkPDPContextActivateSR3GWindow', 'Network Initiated PDP Context Activation Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessPDPActivationEvents/#TotalPDPActivationEvents', 'NWKPDPActSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 23, 'ImsiGroupPDPContextDeactivate3GWindow', 'PDP Context Cutoff Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#networkInitiatedDeactivations/#successfulActivations', 'PDPCutoff')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 24, 'SgsnGroupPDPContextDeactivate3GWindow', 'PDP Context Cutoff Ratio - SGSN-MME group', 1, 1, '100[%]*#networkInitiatedDeactivations/#successfulActivations', 'PDPCutoff')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 25, 'ImsiGroupDtMsPDPContextActivateSR3GWindow', '3GDT MS initiated PDP Context Activation Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessPDPActivationEvents/#TotalPDPActivationEvents', '3GDTMSPDPActSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 26, 'ImsiGroupDtNetworkPDPContextActivateSR3GWindow', '3GDT Network Initiated PDP Context Activation Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessPDPActivationEvents/#TotalPDPActivationEvents', '3GDTNWKPDPActSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 27, 'PlmnGroupMsPDPContextActivateSR3GWindow', 'MS initiated PDP Context Activation Success Ratio - PLMN group', 1, 1, '100[%]*#SuccessPDPActivationEvents/#TotalPDPActivationEvents', 'MSPDPActSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 28, 'PlmnGroupNetworkPDPContextActivateSR3GWindow', 'Network Initiated PDP Context Activation Success Ratio - PLMN group', 1, 1, '100[%]*#SuccessPDPActivationEvents/#TotalPDPActivationEvents', 'NWKPDPActSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 29, 'ImsiGroupDtPDPContextDeactivate3GWindow', '3GDT PDP Context Cutoff Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#networkInitiatedDeactivations/#successfulActivations', '3GDTPDPCutoff')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 30, 'SgsnGroupDtPDPContextDeactivate3GWindow', '3GDT PDP Context Cutoff Ratio - SGSN-MME group', 1, 1, '100[%]*#networkInitiatedDeactivations/#successfulActivations', '3GDTPDPCutoff')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 31, 'PlmnGroupMsServiceRequestSR3GWindow', 'MS initiated Service Request Success Ratio - PLMN group', 1, 1, '100[%]*#SuccessfulUEInitiatedServiceRequestEvents/#TotalUEInitiatedServiceRequestEvents', 'MSSRVREQSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 32, 'PlmnGroupNetworkServiceRequestSR3GWindow', 'Network initiated Service Request Success Ratio - PLMN group', 1, 1, '100[%]*#SuccessfulPagingEvents/#TotalPagingEvents', 'NWKSRVREQSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 33, 'PlmnGroupIsrauSR3GWindow', 'Inter SGSN Routing Area Update Success Ratio - PLMN group', 1, 1, '100[%]*#SuccessISRAUEvents/#TotalISRAUEvents', 'ISRAUSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 34, 'PlmnGroupRauSR3GWindow', 'Intra SGSN Routing Area Update Success Ratio - PLMN group', 1, 1, '100[%]*#SuccessRAUEvents/#TotalRAUEvents', 'RAUSR')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 35, 'ImsiGroupCallForwardingSRMSSWindow', 'Call Forwarding Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessFC/#TotalFC', 'CallForwardingSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 36, 'ImsiGroupRoamingCallForwardingSRMSSWindow', 'Roaming Call Forwarding Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessRCF/#TotalRCF', 'RoamingSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 37, 'MssGroupCallForwardingSRMSSWindow', 'Call Forwarding Success Ratio - MSS group', 1, 1, '100[%]*#SuccessFC/#TotalFC', 'CallForwardingSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 38, 'MssGroupRoamingCallForwardingSRMSSWindow', 'Roaming Call Forwarding Success Ratio - MSS group', 1, 1, '100[%]*#SuccessRCF/#TotalRCF', 'RoamingSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 39, 'ImsiGroupLocationServicesSRMSSWindow', 'Location Request Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessLCS/#TotalLCS', 'LocationRequestsSuccessRate')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 40, 'MssGroupLocationServicesSRMSSWindow', 'Location Request Success Ratio - MSS group', 1, 1, '100[%]*#SuccessLCS/#TotalLCS', 'LocationRequestsSuccessRate')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 41, 'ImsiGroupMsOriginatingSmsinMscSRMSSWindow', 'SMS Originating Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessSMSO/#TotalSMSO', 'SMSOSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 42, 'ImsiGroupMsOriginatingSRMSSWindow', 'MS Originating Call Completion Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessMOC/#TotalMOC', 'MSOCallCompletionRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 43, 'MssGroupMsOriginatingSmsinMscSRMSSWindow', 'SMS Originating Success Ratio - MSS group', 1, 1, '100[%]*#SuccessSMSO/#TotalSMSO', 'SMSOSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 44, 'MssGroupMsOriginatingSRMSSWindow', 'MS Originating Call Completion Ratio - MSS group', 1, 1, '100[%]*#SuccessMOC/#TotalMOC', 'MSOCallCompletionRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 45, 'ImsiGroupMsTerminatingSmsinMscSRMSSWindow', 'SMS Terminating Success Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessSMST/#TotalSMST', 'SMSTSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 46, 'ImsiGroupMsTerminatingSRMSSWindow', 'MS Terminating Call Completion Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessMTC/#TotalMOC', 'MSTCallCompletionRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 47, 'MssGroupMsTerminatingSmsinMscSRMSSWindow', 'SMS Terminating Success Ratio - MSS group', 1, 1, '100[%]*#SuccessSMST/#TotalSMST', 'SMSTSuccessRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 48, 'MssGroupMsTerminatingSRMSSWindow', 'MS Terminating Call Completion Ratio - MSS group', 1, 1, '100[%]*#SuccessMTC/#TotalMOC', 'MSTCallCompletionRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 49, 'ImsiGroupMsTerminatingDRMSSWindow', 'MS Terminating Call Drop Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#DroppedMTC/#TotalMTC', 'MSTCallDropRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 50, 'MssGroupMsTerminatingDRMSSWindow', 'MS Terminating Call Drop Ratio - MSS group', 1, 1, '100[%]*#DroppedMTC/#TotalMTC', 'MSTCallDropRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 51, 'MssGroupMsOriginatingDRMSSWindow', 'MS Originating Call Drop Ratio - MSS group', 1, 1, '100[%]*#DroppedMOC/#TotalMOC', 'MSOCallDropRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 52, 'ImsiGroupMsOriginatingDRMSSWindow', 'MS Originating Call Drop Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#DroppedMOC/#TotalMOC', 'MSOCallDropRatio')        
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 53, 'ImsiGroupCallForwardingDRMSSWindow', 'Call Forwarding Drop Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#DroppedFC/#TotalFC', 'CallForwardingDropRatio')		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 54, 'ImsiGroupRoamingCallForwardingDRMSSWindow', 'Roaming Call Forwarding Drop Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#DroppedRCF/#TotalRCF', 'RoamingDropRatio')		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 55, 'MssGroupCallForwardingDRMSSWindow', 'Call Forwarding Drop Ratio - MSS group', 1, 1, '100[%]*#DroppedFC/#TotalFC', 'CallForwardingDropRatio')		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 56, 'MssGroupRoamingCallForwardingDRMSSWindow', 'Roaming Call Forwarding Drop Ratio - MSS group', 1, 1, '100[%]*#DroppedRCF/#TotalRCF', 'RoamingDropRatio')
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 57, 'ImsiGroupMsOriginatingEmergencyCallDRMSSWindow', 'MS Originating Emergency Call Drop Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#DroppedMOEC/#SuccessMOEC', 'MSOriginatingEmergencyCallDropRatio')		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 58, 'ImsiGroupMsOriginatingEmergencyCallSRMSSWindow', 'MS Originating Emergency Call Completion Ratio - Subscriber (IMSI) group', 1, 1, '100[%]*#SuccessMOEC/#TotalMOEC', 'MSOriginatingEmergencyCallCompletionRatio')		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 59, 'MssGroupMsOriginatingEmergencyCallDRMSSWindow', 'MS Originating Emergency Call Drop Ratio - MSS group', 1, 1, '100[%]*#DroppedMOEC/#SuccessMOEC', 'MSOriginatingEmergencyCallDropRatio')		
		
	insert into WindowDefinition (WINDOWID, NAME, KPINAME, ROPTYPE, ENABLED, KPIFORMULA, SHORTKPINAME)
		values ( 60, 'MssGroupMsOriginatingEmergencyCallSRMSSWindow', 'MS Originating Emergency Call Completion Ratio - MSS group', 1, 1, '100[%]*#SuccessMOEC/#TotalMOEC', 'MSOriginatingEmergencyCallCompletionRatio')	

end

if(select count(*) from sys.systable where table_name = 'RuleDefinition') = 1
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

	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  21, 21, 'ImsiGroupMsPDPContextActivateSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  22, 22, 'ImsiGroupNetworkPDPContextActivateSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  23, 23, 'ImsiGroupPDPContextDeactivate3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  24, 24, 'SgsnGroupPDPContextDeactivate3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  25, 25, 'ImsiGroupDtMsPDPContextActivateSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  26, 26, 'ImsiGroupDtNetworkPDPContextActivateSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  27, 27, 'PlmnGroupMsPDPContextActivateSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  28, 28, 'PlmnGroupNetworkPDPContextActivateSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  29, 29, 'ImsiGroupDtPDPContextDeactivate3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  30, 30, 'SgsnGroupDtPDPContextDeactivate3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  31, 31, 'PlmnGroupMsServiceRequestSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  32, 32, 'PlmnGroupNetworkServiceRequestSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  33, 33, 'PlmnGroupIsrauSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  34, 34, 'PlmnGroupRauSR3GRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  35, 35, 'ImsiGroupCallForwardingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  36, 36, 'ImsiGroupRoamingCallForwardingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  37, 37, 'MssGroupCallForwardingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  38, 38, 'MssGroupRoamingCallForwardingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  39, 39, 'ImsiGroupLocationServicesSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  40, 40, 'MssGroupLocationServicesSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  41, 41, 'ImsiGroupMsOriginatingSmsinMscSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  42, 42, 'ImsiGroupMsOriginatingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  43, 43, 'MssGroupMsOriginatingSmsinMscSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  44, 44, 'MssGroupMsOriginatingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  45, 45, 'ImsiGroupMsTerminatingSmsinMscSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  46, 46, 'ImsiGroupMsTerminatingSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  47, 47, 'MssGroupMsTerminatingSmsinMscSRMSSRule', 'float', 'ratio','<', '98.8')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  48, 48, 'MssGroupMsTerminatingSRMSSRule', 'float', 'dropRatio','>', '0.2')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  49, 49, 'ImsiGroupMsTerminatingDRMSSRule', 'float', 'dropRatio','>', '0.2')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  50, 50, 'MssGroupMsTerminatingDRMSSRule', 'float', 'dropRatio','>', '0.2')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  51, 51, 'MssGroupMsOriginatingDRMSSRule', 'float', 'dropRatio','>', '0.2')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  52, 52, 'ImsiGroupMsOriginatingDRMSSRule', 'float', 'dropRatio','>', '0.2')        
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  53, 53, 'ImsiGroupCallForwardingDRMSSRule', 'float', 'dropRatio','>', '0.2')		
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  54, 54, 'ImsiGroupRoamingCallForwardingDRMSSRule', 'float', 'dropRatio','>', '0.2')		
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  55, 55, 'MssGroupCallForwardingDRMSSRule', 'float', 'dropRatio','>', '0.2')		
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  56, 56, 'MssGroupRoamingCallForwardingDRMSSRule', 'float', 'dropRatio','>', '0.2')
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  57, 57, 'ImsiGroupMsOriginatingEmergencyCallDRMSSRule', 'float', 'dropRatio','>', '0.2')		
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  58, 58, 'ImsiGroupMsOriginatingEmergencyCallSRMSSRule', 'float', 'ratio','<', '98.8')		
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  59, 59, 'MssGroupMsOriginatingEmergencyCallDRMSSRule', 'float', 'dropRatio','>', '0.2')		
		
	insert into RuleDefinition (RULEID, WINDOWID, NAME, DATATYPE, FIELDNAME, OPTYPE, THRESHOLD)
		values(  60, 60, 'MssGroupMsOriginatingEmergencyCallSRMSSRule', 'float', 'ratio','<', '98.8')
		
end