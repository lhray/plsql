@echo off
rem ------------------------------------------------------------------------------------------------------------------------------------------
rem 
rem 
rem Usage:
rem 	FM_OracleImportDB.bat [FromUser] [ToUser] [ToUserPassword] [TargetServiceName] [DmpFileFolder] [DmpFileName]
rem 
rem   For Example
rem		FM_OracleImportDB.bat fmuser_src fmuser_tar futurmaster 10.86.0.129:1521/fmorcl F:\DBHelpers\Oracle\ExpImp\ fmuser_src.dmp
rem 
rem ------------------------------------------------------------------------------------------------------------------------------------------


rem import dba role target db  config

set fromuser=%1
set touser=%2
set touserpassword=%3
set target_service_name=%4
set import_dmp_dir=%5
set import_dmp_filename=%6


if "%1" == "" (
set fromuser=fmuser
set touser=fmuser
set touserpassword=futurmaster
set target_service_name=10.86.0.14:1521/fmorcl
set import_dmp_dir=.\
set import_dmp_filename=%fromuser%.dmp
set prompt_flag=Y
)

if "%1" neq "" (

set prompt_flag=N
)


if %prompt_flag%==Y (
rem input target db dba role config
rem set/p impdbauser=Please input target database sys username[press enter is default value:%impdbauser%]:
rem set/p impdbapassword=Please input target database sys user of password[press enter is default value:%impdbapassword%]:
set/p fromuser=Please input source database username[press enter is default value:%fromuser%]:
set/p touser=Please input target database username[press enter is default value:%touser%]:
set/p touserpassword=Please input target database user password[press enter is default value:%touserpassword%]:
set/p target_service_name=Please input target database of service name[press enter is default value:%target_service_name%]:
set/p import_dmp_dir=Please input target database dmp file directory[press enter is default value:%import_dmp_dir%]:
set/p import_dmp_filename=Please input target database dmp file name[press enter is default value:%import_dmp_filename%]:
)



if /i %touser%==SYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==SYSTEM (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==SCOTT (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==ORACLE_OCM (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==XS$NULL (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==MDDATA (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==DIP (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==APEX_PUBLIC_USER (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==SPATIAL_CSW_ADMIN_USR (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==SPATIAL_WFS_ADMIN_USR (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==DBSNMP (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==SYSMAN (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==FLOWS_FILES (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==MDSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==ORDSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==EXFSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==WMSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==APPQOSSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==APEX_030200 (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==OWBSYS_AUDIT (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==ORDDATA (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==CTXSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==ANONYMOUS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==XDB (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==ORDPLUGINS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==OWBSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==SI_INFORMTN_SCHEMA (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==OLAPSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %touser%==MGMT_VIEW (
  echo  error: input username is oracle sys user!
  pause
  exit
)

if /i %touser%==OUTLN (
  echo  error: input username is oracle sys user!
  pause
  exit
)


rem create user tablespace and so on

rem echo declare >>%import_dmp_dir%\grant.sql
rem echo type T_CURSOR is ref cursor;>>%import_dmp_dir%\grant.sql
rem echo   V_CUR T_CURSOR;>>%import_dmp_dir%\grant.sql
rem echo   str_command varchar2(200);>>%import_dmp_dir%\grant.sql
rem echo   i number:=0;>>%import_dmp_dir%\grant.sql
rem echo   v_default_tablespace varchar2(50);>>%import_dmp_dir%\grant.sql
rem echo   str_text varchar2(500);>>%import_dmp_dir%\grant.sql
rem echo begin>>%import_dmp_dir%\grant.sql
rem echo   select count(1)>>%import_dmp_dir%\grant.sql
rem echo     into i>>%import_dmp_dir%\grant.sql
rem echo    from dba_users t>>%import_dmp_dir%\grant.sql
rem echo    where t.username = upper('%touser%');>>%import_dmp_dir%\grant.sql
rem echo   if i=1 then>>%import_dmp_dir%\grant.sql
rem echo     for V_CUR in (select SID,SERIAL^# from v^$session t where t.USERNAME='FMUSER_ZJH_1233' and t.status^<^>'KILLED') loop >>%import_dmp_dir%\grant.sql
rem echo     str_command:='alter system kill session '''^|^|to_char(V_CUR.SID)^|^|','^|^|to_char(V_CUR.SERIAL#)^|^|'''';>>%import_dmp_dir%\grant.sql
rem echo     execute immediate str_command;>>%import_dmp_dir%\grant.sql
rem echo     end loop;>>%import_dmp_dir%\grant.sql
rem echo      select t.default_tablespace into v_default_tablespace from dba_users t where t.username=upper('%touser%');>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'drop user %touser% cascade';>>%import_dmp_dir%\grant.sql
rem echo      str_text:='create user %touser% identified by futurmaster default tablespace '^|^|v_default_tablespace;>>%import_dmp_dir%\grant.sql 
rem echo      execute immediate str_text;>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant connect,resource to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create view to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create procedure to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create job to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant debug connect session to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create materialized view to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant connect,resource to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create view to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create procedure to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create materialized view to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create any table to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant execute on dbms_pipe to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant execute on dbms_lock to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant execute on dbms_job to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant debug connect session to %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant create any sequence  to  %touser%';>>%import_dmp_dir%\grant.sql
rem echo      execute immediate 'grant dba to %touser%';>>%import_dmp_dir%\grant.sql 
rem echo   end if;>>%import_dmp_dir%\grant.sql
rem echo end;>>%import_dmp_dir%\grant.sql
rem echo />>%import_dmp_dir%\grant.sql
rem echo exit>>%import_dmp_dir%\grant.sql


echo declare >>%import_dmp_dir%\grant.sql
echo     type t_cursor is ref cursor; >>%import_dmp_dir%\grant.sql
echo     c_obj  t_cursor;     >>%import_dmp_dir%\grant.sql
echo     v_sql varchar2(1000);   >>%import_dmp_dir%\grant.sql
echo begin   >>%import_dmp_dir%\grant.sql
echo      for c_obj in (select  T.OBJECT_TYPE,T.OBJECT_NAME from user_objects t) loop   >>%import_dmp_dir%\grant.sql
echo       if    c_obj.object_type not in('INDEX','LOB') then    >>%import_dmp_dir%\grant.sql
echo        begin   >>%import_dmp_dir%\grant.sql
echo         v_sql :='drop '^|^|c_obj.object_type^|^|' ' ^|^|c_obj.object_name;   >>%import_dmp_dir%\grant.sql
echo         if  c_obj.object_type='TABLE' then   >>%import_dmp_dir%\grant.sql
echo           v_sql :=v_sql^|^|' CASCADE CONSTRAINTS PURGE';   >>%import_dmp_dir%\grant.sql
echo         end if;   >>%import_dmp_dir%\grant.sql
echo         execute immediate  v_sql;     >>%import_dmp_dir%\grant.sql
echo         exception when others then    >>%import_dmp_dir%\grant.sql
echo              null;  >>%import_dmp_dir%\grant.sql
echo        end;   >>%import_dmp_dir%\grant.sql
echo       end if;         >>%import_dmp_dir%\grant.sql
echo      end loop;   >>%import_dmp_dir%\grant.sql

echo execute immediate 'create or replace type FMT_obj_nodeid is object(id number)'; >>%import_dmp_dir%\grant.sql
echo execute immediate 'create or replace type FMT_nest_tab_nodeid is table  of FMT_obj_nodeid'; >>%import_dmp_dir%\grant.sql
rem echo execute immediate 'create or replace type FMT_NodeAttriValue is object (NodeID number, AttriValueID number)'; >>%import_dmp_dir%\grant.sql
rem echo execute immediate 'create or replace type FMT_NodeAttriValue_Array is table of FMT_NodeAttriValue'; >>%import_dmp_dir%\grant.sql
echo end;    >>%import_dmp_dir%\grant.sql 
echo />>%import_dmp_dir%\grant.sql
echo exit>>%import_dmp_dir%\grant.sql

sqlplus %touser%/%touserpassword%@%target_service_name% @%import_dmp_dir%\grant.sql             

sqlplus %touser%/%touserpassword%@%target_service_name% @.\recreate_type.sql

sqlplus %touser%/%touserpassword%@%target_service_name% @.\createDB.sql

del %import_dmp_dir%\grant.sql
rem pause
rem imp %touser%/%touserpassword%@%target_service_name% fromuser=%fromuser% touser=%touser% TOID_NOVALIDATE=(%touser%.FMT_obj_nodeid, %touser%.FMT_nest_tab_nodeid,%touser%.FMT_NodeAttriValue,%touser%.FMT_NodeAttriValue_Array) statistics=none ignore=y file=%import_dmp_dir%\%import_dmp_filename% log=%import_dmp_dir%\%import_dmp_filename%.log
imp %touser%/%touserpassword%@%target_service_name% feedback=10000 fromuser=%fromuser% touser=%touser% TOID_NOVALIDATE=(%touser%.FMT_obj_nodeid, %touser%.FMT_nest_tab_nodeid, %touser%.FMT_NODE_TIMESERIES, %touser%.FMT_TYPE, %touser%.FMT_NODEATTRIVALUE, %touser%.FMT_NODEATTRIVALUE_ARRAY,%touser%.FMT_tnlists,%touser%.FMT_NODETS,%touser%.FMT_NODETIMESERIES) statistics=none ignore=y buffer=409600000 file=%import_dmp_dir%\%import_dmp_filename% log=%import_dmp_dir%\%import_dmp_filename%.log

rem pause