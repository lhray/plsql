@echo off
SET CurrentTempFolder=%tmp%
SET ReplaceTargetStr='C:\Users\wjin\AppData\Local\Temp'
SET Logpath=FMSqlldrTest.Log
SET Folder1=FMDATALOADER
SET Folder2=FMPutSerieData
SET SqlBatch=CreateTable.bat
SET SqlBatch2=Output.bat
set fromuser=%1
set fromuserpassword=%2
set Source_service_name=%3

echo %CurrentTime%
echo ====%date% %time% %0 start==== 
echo ====%date% %time% %0 start==== > %Logpath%

Call :CreateFolder %Folder1%
Call :CreateFolder %Folder2%

Call :CopyFile 20000432c_TBMID260258_0000432C.CTL %CurrentTempFolder%\%Folder1%\
Call :CopyFile 20000432C.txt %CurrentTempFolder%\%Folder2%\

REM ReplaceFileStr
echo Call ReplaceFileStr.exe %CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.CTL %CurrentTempFolder% >> %Logpath%
Call ReplaceFileStr.exe %CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.CTL %CurrentTempFolder%
if %ERRORLEVEL% EQU 0 (echo Call ReplaceFileStr.exe success >> %Logpath% ) else (echo Call ReplaceFileStr.exe fail >> %Logpath%)

echo Call %SqlBatch% %fromuser% **** @%Source_service_name% >> %Logpath%
Call %SqlBatch% %fromuser% %fromuserpassword% %Source_service_name%
if %ERRORLEVEL% EQU 0 (echo Call%SqlBatch% success >> %Logpath% ) else (echo Call %SqlBatch% fail with error code %ERRORLEVEL% >> %Logpath%)

echo Call SQLLDR.EXE userid=%fromuser%/**** @%Source_service_name% control='%CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.CTL' errors=100000 log='%CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.LOG' DIRECT=TRUE >> %Logpath%
Call SQLLDR.EXE userid=%fromuser%/%fromuserpassword%@%Source_service_name% control='%CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.CTL' errors=100000 log='%CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.LOG' DIRECT=TRUE
if %ERRORLEVEL% EQU 0 (echo Call SQLLDR.exe success >> %Logpath% ) else (echo Call SQLLDR.exe fail with error code %ERRORLEVEL% >> %Logpath%)

Call :CopyFile %CurrentTempFolder%\%Folder1%\20000432c_TBMID260258_0000432C.LOG %cd%\

echo Call %SqlBatch2% %fromuser% **** %Source_service_name% >> %Logpath%
Call %SqlBatch2% %fromuser% %fromuserpassword% %Source_service_name%
if %ERRORLEVEL% EQU 0 (echo Call%SqlBatch2% success >> %Logpath% ) else (echo Call %SqlBatch2% fail with error code %ERRORLEVEL% >> %Logpath%)

REM SET %id%=FM
REM for %%a in (%FMAPPpath%) do set FM_disk=%%~da
REM %FM_disk%
REM cd %FMAPPpath%
goto :End

:CreateFolder
SET SubFolder=%1
REM "If folder exist,return Sucess 2. If not exist, create OK, return sucess, other case return fail"
if exist %CurrentTempFolder%\%SubFolder% (
   echo %CurrentTempFolder%\%SubFolder% already exist >> %Logpath%
) else (
	echo mkdir %CurrentTempFolder%\%SubFolder% >> %Logpath%
	mkdir %CurrentTempFolder%\%SubFolder%
	if %ERRORLEVEL% EQU 0 (echo create %SubFolder% success >> %Logpath% ) else (echo create %SubFolder% fail >> %Logpath%)
) 
REM echo mkdir %CurrentTempFolder%\%SubFolder% >> %Logpath%
REM mkdir %CurrentTempFolder%\%SubFolder%
REM if %ERRORLEVEL% EQU 0 (echo create %SubFolder% success >> %Logpath% ) else (echo create %SubFolder% fail >> %Logpath%)
Goto :eof

:CopyFile
SET TargetFile=%1
SET DestinationPath=%2
echo copy /Y %TargetFile% to %DestinationPath% >> %Logpath%
copy /Y %TargetFile% %DestinationPath%
if %ERRORLEVEL% EQU 0 (echo CopyFile %TargetFile% success >> %Logpath% ) else (echo CopyFile %TargetFile% fail >> %Logpath%)
Goto :eof

:ExcuteSqlbat
set fromuser=%1
set fromuserpassword=%2
set Source_service_name=%3
echo Call FMOracleServer_Installation.bat %fromuser% %fromuserpassword% %Source_service_name% >> %Logpath%
REM Call FMOracleServer_Installation.bat %fromuser% %fromuserpassword% %Source_service_name%
if %ERRORLEVEL% EQU 0 (echo ExcuteSqlbat success >> %Logpath% ) else (echo ExcuteSqlbat fail >> %Logpath%)
Goto :eof

:End
echo ==== %date% %time% %0 end ==== >> %Logpath%
echo ==== %date% %time% %0 end ====
