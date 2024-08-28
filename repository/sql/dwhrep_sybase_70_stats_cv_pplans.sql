if not exists (select 1 from PartitionPlan where partitionplan='cv_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('cv_day',1095,1800,1095,0)
end if;

if exists (select 1 from PartitionPlan where partitionplan='cv_raw') then
 update partitionplan
 set DEFAULTSTORAGETIME = 30,
 DEFAULTPARTITIONSIZE = 168,
 MAXSTORAGETIME = 90
 where partitionplan = 'cv_raw' 
end if;