/*==============================================================*/
/* dwhrep Forigen Keys	                                        */
/*==============================================================*/
/*==============================================================*/
/* dwhrep_sybase_1 Forigen Keys                                 */
/*==============================================================*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_AGGREGAT_REFERENCE_VERSIONI') = 0
begin
alter table dwhrep.Aggregation
   add foreign key FK_AGGREGAT_REFERENCE_VERSIONI (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_AGGREGAT_AGGREGATI_AGGREGAT') = 0
begin
alter table dwhrep.AggregationRule
   add foreign key FK_AGGREGAT_AGGREGATI_AGGREGAT (AGGREGATION, VERSIONID)
      references dwhrep.Aggregation (AGGREGATION, VERSIONID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_AGGREGAT_TARGET_OF_MEASUREM') = 0
begin
alter table dwhrep.AggregationRule
   add foreign key FK_AGGREGAT_TARGET_OF_MEASUREM (TARGET_MTABLEID)
      references dwhrep.MeasurementTable (MTABLEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementData_To_StorageInfo') = 0
begin
alter table dwhrep.MeasurementColumn
   add foreign key FK_MeasurementData_To_StorageInfo (MTABLEID)
      references dwhrep.MeasurementTable (MTABLEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MEASUREM_MEASUREME_MEASUREM') = 0
begin
alter table dwhrep.MeasurementCounter
   add foreign key FK_MEASUREM_MEASUREME_MEASUREM (TYPEID)
      references dwhrep.MeasurementType (TYPEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MEASUREM_MEASUREME_MEASUREM') = 0
begin
alter table dwhrep.MeasurementKey
   add foreign key FK_MEASUREM_MEASUREME_MEASUREM (TYPEID)
      references dwhrep.MeasurementType (TYPEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementTable_To_MeasurementType') = 0
begin
alter table dwhrep.MeasurementTable
   add foreign key FK_MeasurementTable_To_MeasurementType (TYPEID)
      references dwhrep.MeasurementType (TYPEID)
      on delete restrict on update restrict
end
/*==============================================================*/
/* drop FK in dwhrep_sybase_27                                  */
/*==============================================================*/
/*alter table MeasurementType                                                */
/*   add foreign key FK_MeasurementType_To_MeasurementTypeClass (TYPECLASSID)*/
/*      references MeasurementTypeClass (TYPECLASSID)                        */
/*      on delete restrict on update restrict                               */

/*==============================================================*/
/* drop FK in dwhrep_sybase_27                                  */
/*==============================================================*/
/*alter table MeasurementType                                   */
/*   add foreign key FK_MEASUREM_MEASUREME_VERSIONI (VERSIONID) */
/*      references Versioning (VERSIONID)                       */
/*      on delete restrict on update restrict                  */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementTypeClass_To_Versioning') = 0
begin
alter table dwhrep.MeasurementTypeClass
   add foreign key FK_MeasurementTypeClass_To_Versioning (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end
/*==============================================================*/
/* drop FK in dwhrep_sybase_31                                  */
/*==============================================================*/
/*alter table ReferenceColumn                                   */
/*   add foreign key FK_REFERENC_REFERENCE_REFERENC (TYPEID)    */
/*      references ReferenceTable (TYPEID)                      */
/*      on delete restrict on update restrict                  */

/*==============================================================*/
/* drop FK in dwhrep_sybase_31                                  */
/*==============================================================*/
/*alter table ReferenceTable                                    */
/*   add foreign key FK_REFERENC_REFERENCE_VERSIONI (VERSIONID) */
/*      references Versioning (VERSIONID)                       */
/*      on delete restrict on update restrict                  */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_Transformer_TO_Versioning') = 0
begin
alter table dwhrep.Transformer
   add foreign key FK_Transformer_TO_Versioning (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_Transformation_TO_Transformer') = 0
begin
alter table dwhrep.Transformation
   add foreign key FK_Transformation_TO_Transformer (TRANSFORMERID)
      references dwhrep.Transformer (TRANSFORMERID)
      on delete restrict on update restrict
end
/*==============================================================*/
/* drop FK_InterfaceMeas_TO_DataInterf in dwhrep_sybase_8       */
/*==============================================================*/
/*alter table InterfaceMeasurement                                 */
/*   add foreign key FK_InterfaceMeas_TO_DataInterf (INTERFACENAME)*/
/*      references DataInterface (INTERFACENAME)                   */
/*      on delete restrict on update restrict                     */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_InterfaceMeas_TO_DataFormat') = 0
begin
alter table dwhrep.InterfaceMeasurement
   add foreign key FK_InterfaceMeas_TO_DataFormat (DATAFORMATID)
      references dwhrep.DataFormat (DATAFORMATID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_InterfaceMeas_TO_Transformer') = 0
begin
alter table dwhrep.InterfaceMeasurement
   add foreign key FK_InterfaceMeas_TO_Transformer (TRANSFORMERID)
      references dwhrep.Transformer (TRANSFORMERID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_DefaultTags_TO_DataFormat') = 0
begin
alter table dwhrep.DefaultTags
   add foreign key FK_DefaultTags_TO_DataFormat (DATAFORMATID)
      references dwhrep.DataFormat (DATAFORMATID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_DataFormat_TO_Versioning') = 0
begin
alter table dwhrep.DataFormat
   add foreign key FK_DataFormat_TO_Versioning (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_DataItem_TO_DataFormat') = 0
begin
alter table dwhrep.DataItem
   add foreign key FK_DataItem_TO_DataFormat (DATAFORMATID)
      references dwhrep.DataFormat (DATAFORMATID)
      on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_2 Forigen Keys                                 */
/*==============================================================*/
/*==============================================================*/
/* drop FK in dwhrep_sybase_30                                  */
/*==============================================================*/
/*alter table ExternalStatement                                    */
/*   add foreign key FK_ExternalStatement_To_Versioning (VERSIONID)*/
/*      references Versioning (VERSIONID)                          */
/*      on delete restrict on update restrict                     */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_ExternalStatementStatus_To_DWHTechPacks') = 0
begin
alter table dwhrep.ExternalStatementStatus
   add foreign key FK_ExternalStatementStatus_To_DWHTechPacks (TECHPACK_NAME)
      references dwhrep.DwhTechPacks (TECHPACK_NAME)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_DWHType_To_DWHTechPacks') = 0
begin
alter table dwhrep.DWHType
   add foreign key FK_DWHType_To_DWHTechPacks (TECHPACK_NAME)
      references dwhrep.DWHTechPacks (TECHPACK_NAME)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_DWHColumn_To_DWHType') = 0
begin
alter table dwhrep.DWHColumn
   add foreign key FK_DWHColumn_To_DWHType (STORAGEID)
      references dwhrep.DWHType (STORAGEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_DWHPartition_To_DWHType') = 0
begin
alter table dwhrep.DWHPartition
   add foreign key FK_DWHPartition_To_DWHType (STORAGEID)
      references dwhrep.DWHType (STORAGEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_AlarmReport_To_AlarmInterface') = 0
begin
alter table dwhrep.AlarmReport
   add foreign key FK_AlarmReport_To_AlarmInterface (INTERFACEID)
      references dwhrep.AlarmInterface (INTERFACEID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_TypeActivation_To_TPActivation') = 0
begin
alter table dwhrep.TypeActivation
   add foreign key FK_TypeActivation_To_TPActivation (TECHPACK_NAME)
      references dwhrep.TPActivation (TECHPACK_NAME)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_AlarmReportParameter_To_AlarmReport') = 0
begin
alter table dwhrep.AlarmReportParameter
   add foreign key FK_AlarmReportParameter_To_AlarmReport (REPORTID)
      references dwhrep.AlarmReport (REPORTID)
      on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_3 Forigen Keys                                 */
/*==============================================================*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_PromptImplementor_TO_Versioning') = 0
begin
alter table dwhrep.PromptImplementor
  add foreign key FK_PromptImplementor_TO_Versioning (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_Prompt_TO_Implementor') = 0
begin
alter table dwhrep.Prompt
  add foreign key FK_Prompt_TO_Implementor (VERSIONID,PROMPTIMPLEMENTORID)
      references dwhrep.PromptImplementor (VERSIONID,PROMPTIMPLEMENTORID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_Option_TO_Implementor') = 0
begin
alter table dwhrep.PromptOption
  add foreign key FK_Option_TO_Implementor (VERSIONID,PROMPTIMPLEMENTORID)
      references dwhrep.PromptImplementor (VERSIONID,PROMPTIMPLEMENTORID)
      on delete restrict on update restrict
end

/*==============================================================*/
/* drop FK_InterfaceTps_TO_DataIntf in dwhrep_sybase_8          */
/*==============================================================*/
/*alter table InterfaceTechpacks                                */
/*  add foreign key FK_InterfaceTps_TO_DataIntf (INTERFACENAME) */
/*  references DataInterface(INTERFACENAME)                     */
/*  on delete restrict on update restrict                      */

/*==============================================================*/
/* dwhrep_sybase_8 Forigen Keys                                 */
/*==============================================================*/
/*alter table interfacemeasurement drop foreign key FK_InterfaceMeas_TO_DataInterf*/
/*alter table interfacetechpacks drop foreign key FK_InterfaceTps_TO_DataIntf*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_InterfaceMeasurement_TO_DataInterface') = 0
begin
alter table dwhrep.InterfaceMeasurement
  add foreign key FK_InterfaceMeasurement_TO_DataInterface (INTERFACENAME,INTERFACEVERSION)
    references dwhrep.DataInterface(INTERFACENAME,INTERFACEVERSION)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_InterfaceTechpacks_TO_DataInterface') = 0
begin
alter table dwhrep.InterfaceTechpacks
  add foreign key FK_InterfaceTechpacks_TO_DataInterface (INTERFACENAME,INTERFACEVERSION)
    references dwhrep.DataInterface(INTERFACENAME,INTERFACEVERSION)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_SupportedVendorRelease_TO_Versioning') = 0
begin
alter table dwhrep.SupportedVendorRelease
  add foreign key FK_SupportedVendorRelease_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_TechPackDependency_TO_Versioning') = 0
begin
alter table dwhrep.TechPackDependency
  add foreign key FK_TechPackDependency_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_InterfaceDependency_TO_DataInterface') = 0
begin
alter table dwhrep.InterfaceDependency
  add foreign key FK_InterfaceDependency_TO_DataInterface (INTERFACEVERSION, INTERFACENAME)
    references dwhrep.DataInterface(INTERFACEVERSION, INTERFACENAME)
    on delete restrict on update restrict
end

/*alter table dwhrep.VectorCounter*/
/*  add foreign key FK_VectorCounter_TO_MeasurementColumn (MTABLEID, DATANAME)*/
/*    references dwhrep.MeasurementColumn(MTABLEID, DATANAME)*/
/*    on delete restrict on update restrict*/

/*==============================================================*/
/* drop FK_InterfaceTps_TO_DataIntf in dwhrep_sybase_22         */
/*==============================================================*/
/*alter table UniverseClass                                     */
/*  add foreign key FK_UniverseClass_TO_Versioning (VERSIONID)  */
/*    references Versioning(VERSIONID)                          */
/*    on delete restrict on update restrict                    */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_VerificationObject_TO_Versioning') = 0
begin
alter table dwhrep.VerificationObject
  add foreign key FK_VerificationObject_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

/*PROBLEM*/  
/*alter table dwhrep.VerificationObject */
/*  add foreign key FK_VerificationObject_TO_UniverseClass (VERSIONID, CLASSNAME)*/
/*    references dwhrep.UniverseClass(VERSIONID, CLASSNAME)*/
/*    on delete restrict on update restrict*/

/*==============================================================*/
/* drop FK in dwhrep_sybase_29                                  */
/*==============================================================*/
/*alter table VerificationCondition                                   */
/*  add foreign key FK_VerificationCondition_TO_Versioning (VERSIONID)*/
/*    references Versioning(VERSIONID)                                */
/*    on delete restrict on update restrict                          */
    
/*PROBLEM*/
/*alter table dwhrep.VerificationCondition*/
/*  add foreign key FK_VerificationCondition_TO_UniverseClass (VERSIONID, CONDITION)*/
/*    references dwhrep.UniverseClass(VERSIONID, CLASSNAME)*/
/*    on delete restrict on update restrict*/

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseTable                                     */
/*  add foreign key FK_UniverseTable_TO_Versioning (VERSIONID)  */
/*    references Versioning(VERSIONID)                          */
/*    on delete restrict on update restrict                    */

/*==============================================================*/
/* drop FK_InterfaceTps_TO_DataIntf in dwhrep_sybase_20         */
/*==============================================================*/
/*alter table UniverseJoin                                      */
/*  add foreign key FK_UniverseJoin_TO_Versioning (VERSIONID)   */
/*    references Versioning(VERSIONID)                          */
/*    on delete restrict on update restrict                    */
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseObject_TO_Versioning') = 0
begin
alter table dwhrep.UniverseObject
  add foreign key FK_UniverseObject_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseCondition_TO_Versioning') = 0
begin
alter table dwhrep.UniverseCondition
  add foreign key FK_UniverseCondition_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_9 Forigen Keys                                 */
/*==============================================================*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementVector_To_MeasurementCounter') = 0
begin
alter table dwhrep.MeasurementVector
  add foreign key FK_MeasurementVector_To_MeasurementCounter (TYPEID, DATANAME)
    references dwhrep.MeasurementCounter (TYPEID, DATANAME)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementDeltaCalcSupport_TO_Measurementtype') = 0
begin
alter table dwhrep.MeasurementDeltaCalcSupport
  add foreign key FK_MeasurementDeltaCalcSupport_TO_Measurementtype (TYPEID)
    references dwhrep.Measurementtype(TYPEID)
    on delete restrict on update restrict
end

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseName                                      */
/*  add foreign key FK_UniverseName_TO_Versioning (VERSIONID)   */
/*    references Versioning(VERSIONID)                          */
/*    on delete restrict on update restrict                    */
    
/*==============================================================*/
/* dwhrep_sybase_10 Forigen Keys                                */
/*==============================================================*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementObjBHSupport_TO_Measurementtype') = 0
begin
alter table dwhrep.MeasurementObjBHSupport
  add foreign key FK_MeasurementObjBHSupport_TO_Measurementtype (TYPEID)
    references dwhrep.Measurementtype(TYPEID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementDeltaCalcSupport_TO_Vendorrelease') = 0
begin
alter table dwhrep.MeasurementDeltaCalcSupport
  add foreign key FK_MeasurementDeltaCalcSupport_TO_Vendorrelease (VERSIONID, VENDORRELEASE)
    references dwhrep.SupportedVendorRelease(VERSIONID, VENDORRELEASE)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_Busyhour_TO_Versioning') = 0
begin
alter table dwhrep.Busyhour
  add foreign key FK_Busyhour_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

/*PROBLEM==RESOLVED, FK IS CREATED IN SYBASE_13=============================*/
/*alter table dwhrep.BusyhourSource                                         */
/*  add foreign key FK_BusyhourSource_TO_Busyhour (VERSIONID,BHLEVEL,BHTYPE)*/
/*    references dwhrep.Busyhour(VERSIONID,BHLEVEL,BHTYPE)                  */
/*    on delete restrict on update restrict                                */
/*==========================================================================*/


/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseTable                                     */
/*  add foreign key FK_UniverseTable_TO_Versioning (VERSIONID)  */
/*    references Versioning(VERSIONID)                          */
/*    on delete restrict on update restrict                    */

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseClass                                   */
/*  add foreign key FK_UniverseClass_TO_Versioning (VERSIONID)*/
/*    references Versioning(VERSIONID)                        */
/*    on delete restrict on update restrict                  */

/*==============================================================*/
/* drop FK in dwhrep_sybase_22                                  */
/*==============================================================*/
/*alter table UniverseObject                                                                   */
/*  add foreign key FK_UniverseObject_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)*/
/*    references UniverseClass(VERSIONID,CLASSNAME,UNIVERSEEXTENSION)                          */
/*    on delete restrict on update restrict                                                   */

/*==============================================================*/
/* drop FK in dwhrep_sybase_22                                  */
/*==============================================================*/
/*alter table UniverseCondition                                                                   */
/*  add foreign key FK_UniverseCondition_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)*/
/*    references UniverseClass(VERSIONID, CLASSNAME,UNIVERSEEXTENSION)                            */
/*    on delete restrict on update restrict                                                      */

/*==============================================================*/
/* drop FK in dwhrep_sybase_22                                  */
/*==============================================================*/
/*alter table UniverseComputedObject                                                                   */
/*  add foreign key FK_UniverseComputedObject_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)*/
/*    references UniverseClass(VERSIONID, CLASSNAME,UNIVERSEEXTENSION)                                 */
/*    on delete restrict on update restrict                                                           */

/*==============================================================*/
/* dwhrep_sybase_13 Forigen Keys                                */
/*==============================================================*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_BusyhourSource_TO_Busyhour') = 0
begin
alter table dwhrep.BusyhourSource
  add foreign key FK_BusyhourSource_TO_Busyhour (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE)
    references dwhrep.Busyhour(VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_BusyhourRankkeys_TO_Busyhour') = 0
begin
alter table dwhrep.BusyhourRankkeys
  add foreign key FK_BusyhourRankkeys_TO_Busyhour (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE)
    references dwhrep.Busyhour(VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE)
    on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_15 Forigen Keys                                */
/*==============================================================*/

/*==============================================================*/
/* drop FK in dwhrep_sybase_29                                  */
/*==============================================================*/
/*alter table VerificationCondition                                  */
/*  add foreign key FK_VerificationCondition_TO_Versioning(VERSIONID)*/
/*    references Versioning(VERSIONID)                               */
/*    on delete restrict on update restrict                         */

/*==============================================================*/
/* dwhrep_sybase_17 Forigen Keys                                */
/*==============================================================*/
/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseParameters                                                                                      */
/*  add foreign key FK_UniverseParameters_TO_UniverseComputedObject (VERSIONID,CLASSNAME,OBJECTNAME,UNIVERSEEXTENSION)*/
/*    references UniverseComputedObject(VERSIONID,CLASSNAME,OBJECTNAME,UNIVERSEEXTENSION)                             */
/*    on delete restrict on update restrict                                                                          */

/*==============================================================*/
/* dwhrep_sybase_20 Forigen Keys                                */
/*==============================================================*/
/*alter table UniverseJoin drop foreign key FK_UniverseJoin_TO_Versioning*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseJoin_TO_Versioning') = 0
begin
alter table dwhrep.UniverseJoin
  add foreign key FK_UniverseJoin_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_22 Forigen Keys                                */
/*==============================================================*/
/*alter table UniverseClass drop foreign key FK_UniverseClass_TO_Versioning*/
/*alter table UniverseObject drop foreign key FK_UniverseObject_TO_UniverseClass*/
/*alter table UniverseCondition drop foreign key FK_UniverseCondition_TO_UniverseClass*/
/*alter table UniverseComputedObject drop foreign key FK_UniverseComputedObject_TO_UniverseClass*/

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseClass                                     */
/*  add foreign key FK_UniverseClass_TO_Versioning (VERSIONID)  */
/*    references Versioning(VERSIONID)                          */
/*    on delete restrict on update restrict                    */

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseObject                                                                   */
/*  add foreign key FK_UniverseObject_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)*/
/*    references UniverseClass(VERSIONID,CLASSNAME,UNIVERSEEXTENSION)                          */
/*    on delete restrict on update restrict                                                   */

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseCondition                                                                   */
/*  add foreign key FK_UniverseCondition_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)*/
/*    references UniverseClass(VERSIONID, CLASSNAME,UNIVERSEEXTENSION)                            */
/*    on delete restrict on update restrict                                                      */

/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseComputedObject                                                                   */
/*  add foreign key FK_UniverseComputedObject_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)*/
/*    references UniverseClass(VERSIONID, CLASSNAME,UNIVERSEEXTENSION)                                 */
/*    on delete restrict on update restrict                                                           */

/*==============================================================*/
/* dwhrep_sybase_24 Forigen Keys                                */
/*==============================================================*/
/*==============================================================*/
/* drop FK in dwhrep_sybase_26                                  */
/*==============================================================*/
/*alter table UniverseParameters                                                                                      */
/*  add foreign key FK_UniverseParameters_TO_UniverseComputedObject (VERSIONID,CLASSNAME,OBJECTNAME,UNIVERSEEXTENSION)*/
/*    references UniverseComputedObject(VERSIONID,CLASSNAME,OBJECTNAME,UNIVERSEEXTENSION)                             */
/*    on delete restrict on update restrict                                                                          */

/*==============================================================*/
/* dwhrep_sybase_26 Forigen Keys                                */
/*==============================================================*/
/*alter table UniverseClass drop foreign key FK_UniverseClass_TO_Versioning*/
/*alter table UniverseComputedObject drop foreign key FK_UniverseComputedObject_TO_UniverseClass*/
/*alter table UniverseCondition drop foreign key FK_UniverseCondition_TO_UniverseClass*/
/*alter table UniverseName drop foreign key FK_UniverseName_TO_Versioning*/
/*alter table UniverseObject drop foreign key FK_UniverseObject_TO_UniverseClass*/
/*alter table UniverseParameters drop foreign key FK_UniverseParameters_TO_UniverseComputedObject*/
/*alter table UniverseTable drop foreign key FK_UniverseTable_TO_Versioning*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseClass_TO_Versioning') = 0
begin
alter table dwhrep.UniverseClass
  add foreign key FK_UniverseClass_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseComputedObject_TO_UniverseClass') = 0
begin
alter table dwhrep.UniverseComputedObject
  add foreign key FK_UniverseComputedObject_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)
    references dwhrep.UniverseClass(VERSIONID, CLASSNAME,UNIVERSEEXTENSION)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseCondition_TO_UniverseClass') = 0
begin
alter table dwhrep.UniverseCondition
  add foreign key FK_UniverseCondition_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)
    references dwhrep.UniverseClass(VERSIONID, CLASSNAME,UNIVERSEEXTENSION)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseName_TO_Versioning') = 0
begin
alter table dwhrep.UniverseName
  add foreign key FK_UniverseName_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseObject_TO_UniverseClass') = 0
begin
alter table dwhrep.UniverseObject
  add foreign key FK_UniverseObject_TO_UniverseClass (VERSIONID, CLASSNAME,UNIVERSEEXTENSION)
    references dwhrep.UniverseClass(VERSIONID,CLASSNAME,UNIVERSEEXTENSION)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseParameters_TO_UniverseComputedObject') = 0
begin
alter table dwhrep.UniverseParameters
  add foreign key FK_UniverseParameters_TO_UniverseComputedObject (VERSIONID,CLASSNAME,OBJECTNAME,UNIVERSEEXTENSION)
    references dwhrep.UniverseComputedObject(VERSIONID,CLASSNAME,OBJECTNAME,UNIVERSEEXTENSION)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseTable_TO_Versioning') = 0
begin
alter table dwhrep.UniverseTable
  add foreign key FK_UniverseTable_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_27 Forigen Keys                                */
/*==============================================================*/
/*alter table MeasurementType drop foreign key FK_MeasurementType_To_MeasurementTypeClass*/
/*alter table MeasurementType drop foreign key FK_MEASUREM_MEASUREME_VERSIONI*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MeasurementType_To_MeasurementTypeClass') = 0
begin
alter table dwhrep.MeasurementType
   add foreign key FK_MeasurementType_To_MeasurementTypeClass (TYPECLASSID)
      references dwhrep.MeasurementTypeClass (TYPECLASSID)
      on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_MEASUREM_MEASUREME_VERSIONI') = 0
begin
alter table dwhrep.MeasurementType
   add foreign key FK_MEASUREM_MEASUREME_VERSIONI (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_29 Forigen Keys                                */
/*==============================================================*/
/*alter table VerificationCondition drop foreign key FK_VerificationCondition_TO_Versioning*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_VerificationCondition_TO_Versioning') = 0
begin
alter table dwhrep.VerificationCondition
  add foreign key FK_VerificationCondition_TO_Versioning(VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_30 Forigen Keys                                */
/*==============================================================*/
/*alter table dwhrep.ExternalStatement drop foreign key FK_ExternalStatement_To_Versioning*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_ExternalStatement_To_Versioning') = 0
begin
alter table dwhrep.dwhrep.ExternalStatement
   add foreign key FK_ExternalStatement_To_Versioning (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_31 Forigen Keys                                */
/*==============================================================*/
/*alter table dwhrep.ReferenceColumn drop foreign key FK_REFERENC_REFERENCE_REFERENC*/
/*alter table dwhrep.ReferenceTable drop foreign key FK_REFERENC_REFERENCE_VERSIONI*/

/*==============================================================*/
/* drop FK in dwhrep_sybase_32                                  */
/*==============================================================*/
/*alter table dwhrep.ReferenceColumn                                                 */
/*       add constraint FK_REFERENC_REFERENCE_REFERENC foreign key (TYPEID, BASEDEF) */
/*       references dwhrep.ReferenceTable (TYPEID, BASEDEF)                          */
/*       on delete restrict on update restrict                                      */

/*==============================================================*/
/* drop FK in dwhrep_sybase_32                                  */
/*==============================================================*/
/*alter table dwhrep.ReferenceTable                             */
/*   add foreign key FK_REFERENC_REFERENCE_VERSIONI (VERSIONID) */
/*      references dwhrep.Versioning (VERSIONID)                */
/*      on delete restrict on update restrict                  */

/*==============================================================*/
/* dwhrep_sybase_32 Forigen Keys                                */
/*==============================================================*/
/*alter table dwhrep.ReferenceColumn drop foreign key FK_REFERENC_REFERENCE_REFERENC*/
/*alter table dwhrep.ReferenceTable drop foreign key FK_REFERENC_REFERENCE_VERSIONI*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_REFERENC_REFERENCE_REFERENC') = 0
begin
alter table dwhrep.dwhrep.ReferenceColumn 
       add constraint FK_REFERENC_REFERENCE_REFERENC foreign key (TYPEID) 
       references dwhrep.ReferenceTable (TYPEID) 
       on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_REFERENC_REFERENCE_VERSIONI') = 0
begin
alter table dwhrep.dwhrep.ReferenceTable
   add foreign key FK_REFERENC_REFERENCE_VERSIONI (VERSIONID)
      references dwhrep.Versioning (VERSIONID)
      on delete restrict on update restrict
end

/*==============================================================*/
/* dwhrep_sybase_41 Forigen Keys                                */
/*==============================================================*/
if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_BusyhourMapping_TO_Busyhour') = 0
begin
alter table dwhrep.BusyhourMapping
  add foreign key FK_BusyhourMapping_TO_Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT)
    references dwhrep.Busyhour(VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_BusyhourMapping_TO_MeasurementObjBHSupport') = 0
begin
alter table dwhrep.BusyhourMapping
  add foreign key FK_BusyhourMapping_TO_MeasurementObjBHSupport (TYPEID, BHOBJECT)
    references dwhrep.MeasurementObjBHSupport(TYPEID, OBJBHSUPPORT)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_BusyhourPlaceholders_TO_Versioning') = 0
begin
alter table dwhrep.BusyhourPlaceholders
  add foreign key FK_BusyhourPlaceholders_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end

if (select count(*) from SYS.SYSFOREIGNKEYS where role = 'FK_UniverseFormulas_TO_Versioning') = 0
begin
alter table dwhrep.UniverseFormulas
  add foreign key FK_UniverseFormulas_TO_Versioning (VERSIONID)
    references dwhrep.Versioning(VERSIONID)
    on delete restrict on update restrict
end
