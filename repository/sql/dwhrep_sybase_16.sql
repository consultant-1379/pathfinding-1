create table UniverseFormulas (
   VERSIONID           varchar(128) not null,
   TECHPACK_TYPE       varchar(32) null,
   NAME                varchar(255) not null,
   FORMULA             varchar(3200) null
      
) @system@;

alter table UniverseFormulas
   add primary key (VERSIONID, NAME);