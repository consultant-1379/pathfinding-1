alter table UniverseJoin drop primary key;

alter table UniverseJoin add TMPCOUNTER int IDENTITY not null ;
alter table UniverseJoin modify TARGETLEVEL varchar(255) null;
alter table UniverseJoin modify SOURCELEVEL varchar(255) null;
alter table UniverseJoin modify EXPRESSION varchar(255)   null;

alter table UniverseJoin
  add primary key (VERSIONID, SOURCETABLE,SOURCECOLUMN, TARGETTABLE, TARGETCOLUMN, TMPCOUNTER);
