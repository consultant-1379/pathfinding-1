/*=====================================================================================*/
/*Update KPIFORMULA for some of the windowdefinitions*/
/*=====================================================================================*/

if(select count(*) from sys.systable where table_name = 'WindowDefinition') = 1
begin
	UPDATE WindowDefinition SET KPIFORMULA = '100[%]*#networkInitiatedDeactivations/#TotalDeactivateEvents' WHERE NAME in ('ImsiGroupPDPContextDeactivate3GWindow', 'SgsnGroupPDPContextDeactivate3GWindow')
end
