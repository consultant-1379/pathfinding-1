--
-- sp_iqconnection - function for ASA to appear like IQ for the purpose of ENIQ monitoring console
--
--

if (object_id('dbo.sp_iqconnection') is not null) then drop procedure dbo.sp_iqconnection ; if (object_id('dbo.sp_iqconnection') is not null) then message '<<< FAILED to drop procedure dbo.sp_iqconnection >>>' type info to client ; else message '<<< DROPPED procedure dbo.sp_iqconnection >>>' type info to client ; end if ; end if ;

create procedure dbo.sp_iqconnection() begin declare @_IQconnHandle varchar(128); declare @_Name varchar(128); declare @_Userid varchar(128); declare @_LastReqTime varchar(128); declare @_ReqType varchar(128); declare @_IQCmdType varchar(128); declare @_LastIQCmdTime varchar(128); declare @_ConnCreateTime varchar(128); declare @_IqConnId varchar(128); declare @_CommLink varchar(128); declare @_NodeAddr varchar(128); declare local temporary table _iqConnection ( IQconnHandle varchar(128), Name varchar(128), Userid varchar(128), LastReqTime varchar(128), ReqType varchar(128), IQCmdType varchar(128), LastIQCmdTime varchar(128), blank8 varchar(128), blank9 varchar(128), blank10 varchar(128), blank11 varchar(128), ConnCreateTime varchar(128), blank13 varchar(128), blank14 varchar(128), IqConnId varchar(128), blank16 varchar(128), blank17 varchar(128), CommLink varchar(128), NodeAddr varchar(128)) in system on commit preserve rows ; for loop1 as cursor1 cursor for select Name, Userid, LastReqTime, ReqType, inf.Number, CommLink, NodeAddr, pro.Value as ConnCreateTime from sa_conn_info() inf left join sa_conn_properties() pro on inf.Number = pro.Number where propName = 'logintime' do set @_IQconnHandle = Number; set @_Name = Name; set @_Userid = Userid; set @_LastReqTime = LastReqTime; set @_ReqType = ReqType; set @_IQCmdType	= ''; set @_LastIQCmdTime	= ''; set @_ConnCreateTime = ConnCreateTime; set @_IqConnId = Number; set @_CommLink = CommLink; set @_NodeAddr = NodeAddr; insert into _iqConnection values ( @_IQconnHandle, @_Name, @_Userid, @_LastReqTime, @_ReqType, @_IQCmdType, @_LastIQCmdTime, '', '', '', '', @_ConnCreateTime, '', '', @_IqConnId, '', '', @_CommLink, @_NodeAddr  ) ; end for ; select * from _iqConnection ; end ;

commit ;

if (object_id('dbo.sp_iqconnection') is not null) then message '<<< CREATED procedure dbo.sp_iqconnection >>>' type info to client ; grant execute on dbo.sp_iqconnection to public ; else message '<<< FAILED to create procedure dbo.sp_iqconnection >>>' type info to client ; end if ;

commit;
