
insert into etlrep.META_EXECUTION_SLOT_PROFILE
 (PROFILE_NAME, PROFILE_ID, ACTIVE_FLAG) values (
  'Normal', '0', 'Y')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '0', 'Slot1', '0', 'adapter,Adapter,Aggregator,Alarm,Install,Loader,Mediation,Topology', 'dwh')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '0', 'Slot2', '1', 'adapter,Adapter,Aggregator,Alarm,Install,Loader,Mediation,Topology', 'dwh')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '0', 'Slot3', '2', 'adapter,Adapter,Aggregator,Alarm,Install,Loader,Mediation,Topology', 'dwh')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '0', 'Slot4', '3', 'Partition,Service,Support', 'dwh')

insert into etlrep.META_EXECUTION_SLOT_PROFILE
 (PROFILE_NAME, PROFILE_ID, ACTIVE_FLAG) values (
  'NoLoads', '1', 'N')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '1', 'Slot1', '4', 'Support,Install,Partition', 'dwh')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '1', 'Slot2', '5', 'Support', 'dwh')

insert into etlrep.META_EXECUTION_SLOT
 (PROFILE_ID, SLOT_NAME, SLOT_ID, ACCEPTED_SET_TYPES, SERVICE_NODE) values (
  '1', 'Slot3', '6', 'Support', 'dwh')
