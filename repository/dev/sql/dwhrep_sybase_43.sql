/*==========================================================================================*/
/* MODIFY: Busyhour 									                */
/*==========================================================================================*/

alter table dwhrep.Busyhour drop primary key;
alter table dwhrep.Busyhour modify BHLEVEL varchar(255) not null;
alter table dwhrep.Busyhour add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE);

/*==========================================================================================*/
/* MODIFY: BusyhourMapping                                                                 */
/*==========================================================================================*/

alter table dwhrep.BusyhourMapping drop primary key;
alter table dwhrep.BusyhourMapping modify BHLEVEL varchar(255) not null;
alter table dwhrep.BusyhourMapping
  add primary key (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, TYPEID);

/*==========================================================================================*/
/* MODIFY: BusyhourPlaceholders                                                             */
/*==========================================================================================*/

alter table dwhrep.BusyhourPlaceholders drop primary key; 
alter table dwhrep.BusyhourPlaceholders modify BHLEVEL varchar(255) not null;
alter table dwhrep.BusyhourPlaceholders add primary key (VERSIONID, BHLEVEL);

/*==========================================================================================*/
/* MODIFY: BusyhourRankkeys                                                                 */
/*==========================================================================================*/

alter table dwhrep.BusyhourRankkeys drop primary key;
alter table dwhrep.BusyhourRankkeys modify BHLEVEL varchar(255) not null;
alter table dwhrep.BusyhourRankkeys add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, KEYNAME);

/*==========================================================================================*/
/* MODIFY: BusyhourSource                                                                   */
/*==========================================================================================*/

alter table dwhrep.BusyhourSource drop primary key;
alter table dwhrep.BusyhourSource modify BHLEVEL varchar(255) not null;
alter table dwhrep.BusyhourSource add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, TYPENAME);

/*==========================================================================================*/
/* MODIFY: DataFormat                                                                       */
/*==========================================================================================*/

alter table dwhrep.DataFormat modify FOLDERNAME varchar(128) not null;

/*==========================================================================================*/
/* MODIFY: DefaultTags                                                                      */
/*==========================================================================================*/

alter table dwhrep.DefaultTags delete primary key;
alter table dwhrep.DefaultTags modify TAGID varchar(128) not null;
alter table dwhrep.DefaultTags add primary key (TAGID, DATAFORMATID);

/*==========================================================================================*/
/* MODIFY: DataInterface                                                                    */
/*==========================================================================================*/

alter table dwhrep.DataInterface drop primary key;
alter table dwhrep.DataInterface modify INTERFACENAME varchar(255) not null;
alter table dwhrep.DataInterface add primary key (INTERFACENAME,INTERFACEVERSION);

/*==========================================================================================*/
/* MODIFY: InterfaceTechpacks                                                               */
/*==========================================================================================*/

alter table dwhrep.InterfaceTechpacks drop primary key;
alter table dwhrep.InterfaceTechpacks modify INTERFACENAME varchar(255) not null;
alter table dwhrep.InterfaceTechpacks add primary key (INTERFACENAME,INTERFACEVERSION,TECHPACKNAME,TECHPACKVERSION);

/*==========================================================================================*/
/* MODIFY: InterfaceDependency                                                             */
/*==========================================================================================*/

alter table dwhrep.InterfaceDependency drop primary key;
alter table dwhrep.InterfaceDependency modify INTERFACENAME varchar(255) not null;
alter table dwhrep.InterfaceDependency
  add primary key (INTERFACENAME,INTERFACEVERSION,TECHPACKNAME,TECHPACKVERSION);

/*==========================================================================================*/
/* MODIFY: InterfaceMeasurement                                                             */
/*==========================================================================================*/

alter table dwhrep.InterfaceMeasurement drop primary key;
alter table dwhrep.InterfaceMeasurement modify TAGID varchar(128) not null;
alter table dwhrep.InterfaceMeasurement modify INTERFACENAME varchar(255) not null;
alter table dwhrep.InterfaceMeasurement add primary key (TAGID,INTERFACENAME,INTERFACEVERSION);

/*==========================================================================================*/
/* MODIFY: MeasurementTypeClass                                                             */                                                      
/*==========================================================================================*/

alter table dwhrep.MeasurementTypeClass drop primary key;
alter table dwhrep.MeasurementTypeClass modify TYPECLASSID varchar(255) not null;
alter table dwhrep.MeasurementTypeClass add primary key (TYPECLASSID);

/*==========================================================================================*/
/* MODIFY: MeasurementType                                                                  */
/*==========================================================================================*/

alter table dwhrep.MeasurementType modify TYPECLASSID varchar(255) not null;
alter table dwhrep.MeasurementType modify VENDORID varchar(128) null;
alter table dwhrep.MeasurementType modify FOLDERNAME varchar(128) null;