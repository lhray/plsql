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




set NLS_LANG=AMERICAN_AMERICA.AL32UTF8

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




sqlplus %touser%/%touserpassword%@%target_service_name% @.\dropAllObject.sql            
sqlplus %touser%/%touserpassword%@%target_service_name% @.\createDB.sql
imp %touser%/%touserpassword%@%target_service_name% feedback=10000 fromuser=%fromuser% touser=%touser% TOID_NOVALIDATE=(%touser%.FMT_obj_nodeid, %touser%.FMT_nest_tab_nodeid, %touser%.FMT_NODE_TIMESERIES, %touser%.FMT_TYPE, %touser%.FMT_NODEATTRIVALUE, %touser%.FMT_NODEATTRIVALUE_ARRAY,%touser%.FMT_tnlists,%touser%.FMT_NODETS,%touser%.FMT_NODETIMESERIES) statistics=none ignore=y buffer=409600000 file=%import_dmp_dir%\%import_dmp_filename% log=%import_dmp_dir%\%import_dmp_filename%.log
sqlplus %touser%/%touserpassword%@%target_service_name% @.\createjob.sql
sqlplus %touser%/%touserpassword%@%target_service_name% @..\gather_statistic.sql

