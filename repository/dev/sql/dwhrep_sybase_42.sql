/*==========================================================================================================================*/
/* Issue: Reduced Delay Alarms                                                           									*/
/*		Adding SIMULTANEOUS column to mark if a report is a reduced delay alarm												*/
/*==========================================================================================================================*/


alter table AlarmReport add SIMULTANEOUS integer null;