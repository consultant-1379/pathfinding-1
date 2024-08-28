if not exists (select 1 from PartitionPlan where partitionplan='vol_1min') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol_1min',6,24,6,0)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='vol_15min') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol_15min',14,168,14,0)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='vol_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol_day',1095,4320,1095,0)
end if;

if not exists (select 1 from PartitionPlan where partitionplan='vol3_15min') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol3_15min',18,72,18,0)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='vol3_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol3_day',1095,4320,1095,0)
end if;

if not exists (select 1 from PartitionPlan where partitionplan='vollarge_15min') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vollarge_15min',20,48,20,0)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='vollarge_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vollarge_day',1095,2160,1095,0)
end if;

if not exists (select 1 from PartitionPlan where partitionplan='vol2_15min') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol2_15min',20,48,20,0)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='vol2_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol2_day',30,336,30,0)
end if;

if not exists (select 1 from PartitionPlan where partitionplan='volextralarge_15min') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('volextralarge_15min',13,24,13,0)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='volextralarge_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('volextralarge_day',30,48,30,0)
end if;

if not exists (select 1 from PartitionPlan where partitionplan='volmedium_day') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('volmedium_day',40,72,40,0)
end if;
