@echo off
REM
REM===============================================
REM=         Create Table                        =
REM===============================================

set username=fmuser_ljy
set password=futurmaster
set service_name=10.86.0.14:1521/fmorcl

set /p Username=Input Username[Default %username%]:
set /p Password=Input Password[Default %password%]:
set /p Service_name=Input Service_name[Default %service_name%]:

  
sqlplus %username%/%password%@%service_name%  @.\Output.sql
exit;
