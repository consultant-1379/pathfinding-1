/*==========================================================================================================================*/
/* Issue: ENIQ 2.5 has 36.sql with only quantity column addition.                                                           */
/*        ENIQ 11 has 36.sql with quantity column + BH tables modifications.      											*/
/*        This script is a fix for supporting upgrade from any version to ENIQ 11.0.10.										*/
/* Content: Removed the BH content from 36.sql and pasted it here.															*/
/*			Added a conditional check to see if BusyhourMapping table already exists. If not then we run ALL the BH content */
/*==========================================================================================================================*/


IF (select count(*) from SYS.SYSTABLE where table_name='BusyhourMapping') = 0
BEGIN
/*==========================================================================================*/
/* DROP: VectorCounter and BusyhourJoinColumn                                               */
/*==========================================================================================*/

drop table VectorCounter

drop table BusyHourJoinColumn


/*==========================================================================================*/
/* CREATE: BusyourMapping                                                                   */
/*==========================================================================================*/

create table BusyhourMapping (
  VERSIONID       varchar(128) not null,
  BHLEVEL         varchar(32)  not null,
  BHTYPE          varchar(32)  not null,
  TARGETVERSIONID varchar(128) not null,
  BHOBJECT        varchar(32)  not null,
  TYPEID          varchar(255) not null,
  BHTARGETTYPE    varchar(128) not null,
  BHTARGETLEVEL   varchar(128) not null,
  ENABLE          integer      not null
) @system@

alter table BusyhourMapping
  add primary key (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, TYPEID)


/*==========================================================================================*/
/* CREATE: BusyourPlaceHolders                                                              */
/*==========================================================================================*/

create table BusyhourPlaceholders (
  VERSIONID             varchar(128) not null,
  BHLEVEL               varchar(32)  not null,
  PRODUCTPLACEHOLDERS   integer      null,
  CUSTOMPLACEHOLDERS    integer      null
) @system@

alter table BusyhourPlaceholders
  add primary key (VERSIONID, BHLEVEL)

/*==========================================================================================*/
/* MODIFY: Busyhour                                                                          */
/*==========================================================================================*/

alter table Busyhour add ENABLE          integer DEFAULT 1 not null
alter table Busyhour add AGGREGATIONTYPE varchar(128)   DEFAULT 'RANKBH_TIMELIMITED' not null
alter table Busyhour add OFFSET          integer DEFAULT 0 not null
alter table Busyhour add WINDOWSIZE      integer DEFAULT 60 not null
alter table Busyhour add LOOKBACK        integer DEFAULT 0 not null
alter table Busyhour add P_THRESHOLD     integer DEFAULT 0 not null
alter table Busyhour add N_THRESHOLD     integer DEFAULT 0 not null
alter table Busyhour add CLAUSE          varchar(32000) DEFAULT '' not null
alter table Busyhour add PLACEHOLDERTYPE varchar(128)   null
alter table Busyhour add GROUPING        varchar(32)    DEFAULT 'None' not null
alter table Busyhour add REACTIVATEVIEWS      integer DEFAULT 0 not null

/*==========================================================================================*/
/* MODIFY: TPActivation                                                                     */
/*==========================================================================================*/

alter table dwhrep.TPActivation add MODIFIED integer default 0 not null

/*==========================================================================================*/
/* MODIFY: AggregationRule                                                                  */
/*==========================================================================================*/

alter table AggregationRule add ENABLE integer null

END 