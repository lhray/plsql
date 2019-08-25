@echo off


rem import dba role target db  config




set fmuser=%1
set fmuserpassword=%2
set target_service_name=%3

if "%1" neq "" (
set prompt_flag=N
)

if "%1" == "" (
set fmuser=fmuser_***
set fmuserpassword=futurmaster
set target_service_name=10.86.0.14:1521/fmorcl
rem set target_service_name_db=10.86.0.14:1521/fmorcl
set prompt_flag=Y
)


if %prompt_flag%==Y (
rem input target db dba role config
rem set/p impdbauser=Please input target database sys username¡¾press enter is default value:%impdbauser%¡¿:
rem, set/p impdbapassword=Please input target database sys user of password¡¾press enter is default value:%impdbapassword%¡¿:
set/p fmuser=Please input  username¡¾press enter is default value:%fmuser%¡¿:
set/p fmuserpassword=Please input  password¡¾press enter is default value:%fmuserpassword%¡¿:
set/p target_service_name=Please input target database of service name¡¾press enter is default value:%target_service_name%¡¿:
rem set/p target_service_name_db=Please input target database connect¡¾press enter is default value:%target_service_name_db%¡¿:
)
rem create user tablespace and so on

rem echo declare >>.\grant.sql
rem echo type T_CURSOR is ref cursor;>>.\grant.sql
rem echo   V_CUR T_CURSOR;>>.\grant.sql
rem echo   str_command varchar2(200);>>.\grant.sql
rem echo   i number:=0;>>.\grant.sql
rem echo   v_default_tablespace varchar2(50);>>.\grant.sql
rem echo   str_text varchar2(500);>>.\grant.sql
rem echo begin>>.\grant.sql
rem echo   select count(1)>>.\grant.sql
rem echo     into i>>.\grant.sql
rem echo    from dba_users t>>.\grant.sql
rem echo    where t.username = upper('%fmuser%');>>.\grant.sql
rem echo   if i=1 then>>.\grant.sql
rem echo     for V_CUR in (select SID,SERIAL^# from v^$session t where t.USERNAME='FMUSER_ZJH_1233' and t.status^<^>'KILLED') loop >>.\grant.sql
rem echo     str_command:='alter system kill session '''^|^|to_char(V_CUR.SID)^|^|','^|^|to_char(V_CUR.SERIAL#)^|^|'''';>>.\grant.sql
rem echo     execute immediate str_command;>>.\grant.sql
rem echo     end loop;>>.\grant.sql
rem echo      select t.default_tablespace into v_default_tablespace from dba_users t where t.username=upper('%fmuser%');>>.\grant.sql
rem echo      execute immediate 'drop user %fmuser% cascade';>>.\grant.sql
rem echo      str_text:='create user %fmuser% identified by futurmaster default tablespace '^|^|v_default_tablespace;>>.\grant.sql 
rem echo      execute immediate str_text;>>.\grant.sql

rem echo      grant connect,resource to %fmuser%;>>.\grant.sql
rem echo      grant create view to %fmuser%;>>.\grant.sql
rem echo      grant create procedure to %fmuser%;>>.\grant.sql
rem echo      grant create job to %fmuser%;>>.\grant.sql
rem echo      grant debug connect session to %fmuser%;>>.\grant.sql
rem echo      grant create materialized view to %fmuser%;>>.\grant.sql
rem echo      grant connect,resource to %fmuser%;>>.\grant.sql
rem echo      grant create view to %fmuser%;>>.\grant.sql
rem echo      grant create procedure to %fmuser%;>>.\grant.sql
rem echo      grant create materialized view to %fmuser%;>>.\grant.sql
rem echo      grant create any table to %fmuser%;>>.\grant.sql
rem echo      grant execute on dbms_pipe to %fmuser%;>>.\grant.sql
rem echo      grant execute on dbms_lock to %fmuser%;>>.\grant.sql
rem echo      grant execute on dbms_job to %fmuser%;>>.\grant.sql
rem echo      grant debug connect session to %fmuser%;>>.\grant.sql
rem echo      grant create any sequence  to  %fmuser%;>>.\grant.sql
rem rem echo      grant dba to %fmuser%;>>.\grant.sql
rem echo      exit;>>.\grant.sql 


rem sqlplus "%impdbauser%/%impdbapassword%@%target_service_name% as sysdba" @.\grant.sql

rem del .\grant.sql

sqlplus %fmuser%/%fmuserpassword%@%target_service_name% @.\FMOracle_DropTempTable.sql

rem pause