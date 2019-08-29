@echo off
REM
REM===============================================
REM=         Create Table                        =
REM===============================================
set username=%1
set password=%2
set service_name=%3

REM set username=fmuser_ljy
REM set password=futurmaster
REM set service_name=10.86.0.14:1521/fmorcl

REM set /p Username=Input Username[Default %username%]:
REM set /p Password=Input Password[Default %password%]:
REM set /p Service_name=Input Service_name[Default %service_name%]:

  
echo sqlplus %username%/%password%@%service_name%  @.\CreateTable.sql
sqlplus %username%/%password%@%service_name%  @.\CreateTable.sql

REM exit;
