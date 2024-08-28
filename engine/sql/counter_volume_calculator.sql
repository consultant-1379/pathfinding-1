/* This will be used to calculate the counter volume. */
declare @a datetime
select @a = max(rop_starttime) from dc.log_session_adapter where rop_starttime <= getdate() and rop_starttime >= getdate()-1
select sum(countersum)/96/1000000 as totalcounter from (
select sum(num_of_counters) as countersum from dc.log_session_adapter where ROP_STARTTIME >= dateadd(day,-1,@a) and ROP_STARTTIME < @a and datediff(minute,rop_starttime,rop_endtime) = 15 union all
select sum(num_of_counters)*1.25 as countersum from dc.log_session_adapter where ROP_STARTTIME >= dateadd(day,-1,@a) and ROP_STARTTIME < @a and datediff(minute,rop_starttime,rop_endtime) = 5 union all
select sum(num_of_counters)*3/4 as countersum from dc.log_session_adapter where ROP_STARTTIME >= dateadd(day,-1,@a) and ROP_STARTTIME < @a and datediff(minute,rop_starttime,rop_endtime) = 30 union all
select sum(num_of_counters)/2 as countersum from dc.log_session_adapter where ROP_STARTTIME >= dateadd(day,-1,@a) and ROP_STARTTIME < @a and datediff(minute,rop_starttime,rop_endtime) = 60 union all
select round(count(distinct UCELL_ID )*1000000/3000,0) as countersum from dc.DIM_E_RAN_UCELL union all
select round(count(distinct EUtranCellId)*1000000/3000,0) as countersum from dc.DIM_E_LTE_Eucell)a