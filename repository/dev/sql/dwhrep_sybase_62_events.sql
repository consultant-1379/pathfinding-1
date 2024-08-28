if not exists (select 1 from PartitionPlan where partitionplan='vol3extralarge_raw') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol3extralarge_raw',2600000000,200000000,2600000000,1)
end if;
if not exists (select 1 from PartitionPlan where partitionplan='vol4medium_raw') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('vol4medium_raw',10200000000,200000000,10200000000,1)
end if;