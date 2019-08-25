rem set sqldevelop bin
set sqlbin=%1

if "%1"=="" (
set sqlbin=d:\sqldeveloper-3.2.20.09.87\sqldeveloper\sqldeveloper\bin
)

set current_root=%CD%

echo Start Test : %TIME%>%current_root%\ut_log.log
rem update sql by svn 
REM cd /D F:\DBHelpers\Oracle
REM svn update

cd /D %current_root%
rem for every ut test file
for %%i in (ut_*.bat) do (

rem init database first
REM cd /D F:\DBHelpers\Oracle
REM %current_root%\init_db.bat

rem run unit test
cd /D %sqlbin%
echo Run %current_root%\%%i :>>%current_root%\ut_log.log
%current_root%\%%i>>%current_root%\ut_log.log
)
cd /D %current_root%
echo End Test : %TIME%>>%current_root%\ut_log.log