ALTER TABLE AggregationRule MODIFY RULETYPE NOT NULL;
alter table AggregationRule drop primary key;
alter table AggregationRule add primary key (AGGREGATION,RULEID,VERSIONID,RULETYPE);
