if not exists (select 1 from PartitionPlan where partitionplan='sgeh2extralarge_raw') then
  insert into PartitionPlan (partitionplan,defaultstoragetime,defaultpartitionsize,maxstoragetime,partitiontype) values ('sgeh2extralarge_raw',20000000000,400000000,20000000000,1)
end if;