
/*==============================================================*/
/* Table: Busyhour                                              */
/*==============================================================*/
alter table Busyhour add TARGETVERSIONID varchar(128) not null;
alter table Busyhour add BHOBJECT varchar(32) not null;
alter table Busyhour add BHELEMENT integer not null;
alter table Busyhour drop primary key;
alter table Busyhour add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE);

/*==============================================================*/
/* Table: BusyhourSource                                        */
/*==============================================================*/
alter table BusyhourSource add TARGETVERSIONID varchar(128) not null;
alter table BusyhourSource add BHOBJECT varchar(32) not null;
alter table BusyhourSource drop primary key;
alter table BusyhourSource add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, TYPENAME);
    
/*==============================================================*/
/* Table: BusyhourRankkeys                                      */
/*==============================================================*/
alter table BusyhourRankkeys add TARGETVERSIONID varchar(128) not null;
alter table BusyhourRankkeys add BHOBJECT varchar(32) not null;
alter table BusyhourRankkeys drop primary key;
alter table BusyhourRankkeys add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, KEYNAME);

/*==============================================================*/
/* Table: BusyhourJoincolumn                                    */
/*==============================================================*/
alter table BusyhourJoincolumn add TARGETVERSIONID varchar(128) not null;
alter table BusyhourJoincolumn add BHOBJECT varchar(32) not null;
alter table BusyhourJoincolumn drop primary key;
alter table BusyhourJoincolumn add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, JOINCOLUMN);
