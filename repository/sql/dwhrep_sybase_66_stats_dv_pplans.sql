if not exists (select 1 from PartitionPlan where partitionplan='cv_raw') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('cv_raw',1095,1800,1095,0)
end if;