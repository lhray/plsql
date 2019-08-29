create or replace view fmv_getoperationlog as
select  substr(l.lsection,instr(l.lsection,'.',-1)+1) proc,
(max(to_date(to_char(l.ldate,'yyyy-mm-dd HH24:mi:ss'),'yyyy-mm-dd HH24:mi:ss'))-
min(to_date(to_char(l.ldate,'yyyy-mm-dd HH24:mi:ss'),'yyyy-mm-dd HH24:mi:ss')))*60*60*24 sec
 from log_operation l
 group by l.lsessionid,
 to_char(l.ldate,'yyyy-mm-dd HH24'),
 substr(l.lsection,instr(l.lsection,'.',-1)+1)
 order by to_char(l.ldate,'yyyy-mm-dd HH24') desc;




create or replace view fmv_alllog as 
select * from log_operation l  order by 1 desc;

create or replace view fmv_debuglog as 
select * from log_operation l where l.llevel=70 order by 1 desc;

create or replace view fmv_infolog as 
select * from log_operation l where l.llevel=60 order by 1 desc;

create or replace view fmv_warnlog as 
select * from log_operation l where l.llevel=50 order by 1 desc;

create or replace view fmv_errorlog as 
select * from log_operation l where l.llevel=40 order by 1 desc;


create or replace view fmv_crucinfolog as 
select * from log_operation l where l.llevel=30 order by 1 desc;


create or replace view fmv_fatallog as 
select * from log_operation l where l.llevel=20 order by 1 desc;


