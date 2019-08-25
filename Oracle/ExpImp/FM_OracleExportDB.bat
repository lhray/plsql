@echo off
rem dba role source db config

rem set fromuser=fmuser
rem set fromuser=fmuser_***
rem set fromuserpassword=futurmaster
rem set Source_service_name=10.86.0.14:1521/fmorcl
rem set Source_service_name=10.86.0.129:1521/fmorcl
rem set export_dmp_dir=.\
rem set export_dmp_filename=%fromuser%.dmp
rem set prompt_flag=Y

set NLS_LANG=AMERICAN_AMERICA.AL32UTF8

set fromuser=%1
set fromuserpassword=%2
set Source_service_name=%3
set export_dmp_dir=.\
set export_dmp_filename=%1.dmp


if "%1"=="" (
set fromuser=fmuser
set fromuserpassword=futurmaster
set Source_service_name=10.86.0.14:1521/fmorcl
set export_dmp_dir=.\
set export_dmp_filename=%fromuser%.dmp

set prompt_flag=Y
)

if "%1" neq "" (
set prompt_flag=N
)




if %prompt_flag%==Y (
rem input dba role source dbconfig
rem set/p expdbauser=Please input Source database sys username [press enter is default value:%expdbauser%]:
rem set/p expdbapassword=Please input Source database sys user of password [press enter is default value:%expdbapassword%]:
set/p fromuser=Please input Source database username[press enter is default value:%fromuser%]:
set/p fromuserpassword=Please input Source database user password[press enter is default value:%fromuserpassword%]:
set/p Source_service_name=Please input Source database of service name[press enter is default value:%Source_service_name%]:

set/p export_dmp_dir=Please input Source file directory[press enter is default value:%export_dmp_dir%]:
set/p export_dmp_filename=Please input Source database dmp filename[press enter is default value:%fromuser%.dmp]:
)

if /i %fromuser%==SYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==SYSTEM (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==SCOTT (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==ORACLE_OCM (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==XS$NULL (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==MDDATA (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==DIP (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==APEX_PUBLIC_USER (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==SPATIAL_CSW_ADMIN_USR (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==SPATIAL_WFS_ADMIN_USR (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==DBSNMP (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==SYSMAN (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==FLOWS_FILES (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==MDSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==ORDSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==EXFSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==WMSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==APPQOSSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==APEX_030200 (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==OWBSYS_AUDIT (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==ORDDATA (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==CTXSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==ANONYMOUS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==XDB (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==ORDPLUGINS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==OWBSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==SI_INFORMTN_SCHEMA (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==OLAPSYS (
  echo  error: input username is oracle sys user!
  pause
  exit
)
if /i %fromuser%==MGMT_VIEW (
  echo  error: input username is oracle sys user!
  pause
  exit
)

if /i %fromuser%==OUTLN (
  echo  error: input username is oracle sys user!
  pause
  exit
)

rem set/p export_dmp_dir=Please input Source database dmp file directory [press enter is default value:%export_dmp_dir%]:
sqlplus %fromuser%/%fromuserpassword%@%Source_service_name% @.\removeJob.sql
exp %fromuser%/%fromuserpassword%@%Source_service_name% owner=%fromuser% statistics=none file=%export_dmp_dir%\%export_dmp_filename% log=%export_dmp_dir%\%export_dmp_filename%.log
sqlplus %fromuser%/%fromuserpassword%@%Source_service_name% @.\createjob.sql  
rem pause
