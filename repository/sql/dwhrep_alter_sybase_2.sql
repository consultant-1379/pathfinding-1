/*=====================================================================================*/
/*Contains all changes to upgrade an Eniq Stats base to include the Eniq Events schema */
/*=====================================================================================*/

/* dwhrep_sybase_40.sql */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_GPMGT_REFERENCE_VERSION') = 0
begin
alter table GroupTypes
   add foreign key FK_GPMGT_REFERENCE_VERSION (VERSIONID)
	  references Versioning (VERSIONID)
	  on delete restrict on update restrict
end
