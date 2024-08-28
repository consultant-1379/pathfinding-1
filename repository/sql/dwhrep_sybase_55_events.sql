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

	UPDATE WindowDefinition SET  KPINAME = 'MS initiated PDP Context Activation Success Ratio - Subscriber (IMSI) group' WHERE WINDOWID = 21

    UPDATE RuleDefinition SET FIELDNAME = 'ratio', OPTYPE ='<', THRESHOLD='98.8' WHERE RULEID = 48
	
		
end